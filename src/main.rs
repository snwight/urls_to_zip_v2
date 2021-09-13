#[macro_use] extern crate rocket;

use std::fs::File;
use std::io::{BufReader, Read, Write}; // Seek, SeekFrom};
use rocket::response::stream::ByteStream;
use rocket::response::stream::stream;
use serde::Deserialize;
use tempfile::NamedTempFile;
use zip::ZipWriter;
use zip::write::FileOptions;
use zip::result::ZipResult;


//===================================== support for our endpoint ===================================/

#[derive(Deserialize, Debug)]
struct UrlFile {
    url: String,
    filename: String,
}

fn parse_manifest() -> std::io::Result<Vec<UrlFile>> {
    // Presumably this is a static file that's updated regularly by another process
    let file = File::open("./tiny_sample_archive.json")?;

    // Create a buffered reader on it because we can't be sure how big it may grow to be
    let reader = BufReader::new(file);

    // Parse the entire JSON array in one gulp, using typed JSON parser magic 
    // Yes, it could be argued that refactoring this to yield short segments would scale better
    let manifest: Vec<UrlFile> = serde_json::from_reader(reader)?;
    Ok(manifest)
}

async fn fetch_and_compress(zip: &mut ZipWriter<NamedTempFile>, options: FileOptions, url: &str, filename: &str) -> ZipResult<()> {
    // Prepare to compress this file, using its original base name
    zip.start_file(filename, options)?;

    // Download chunkwise and zip into the archive
    let mut res = reqwest::get(url).await.expect("failed to connect to url");
    while let Some(chunk) = res.chunk().await.expect("failed to fetch image chunk") {
        zip.write(&chunk)?;
    };

    Ok(())
}

//===================================== the endpoint in question ===================================/

#[get("/stream/images")]
async fn stream_img_files() -> ByteStream![Vec<u8>] {

    // Load the JSON file 
    let manifest = parse_manifest().expect("problem parsing manifest");

    // Anonymous I/O-optimized tempfile is perfect as a dumping ground for zipped contents
    let archive = NamedTempFile::new().expect("problem creating TempFile");
    let mut tmp_arc = File::open(archive.path()).expect("couldn't obtain archive file handle");
    let mut zip = ZipWriter::new(archive);
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    ByteStream::from(stream! {
        // Iterate over each url:basefilename pair, downloading/compressing/appending on a file-wise basis
        for url_file in manifest.iter() {
            fetch_and_compress(&mut zip, options, &url_file.url, &url_file.filename)
                .await
                .expect("failed to fetch image file");

            // Read arc file from previous offset to current EOF
            let mut buffer: Vec<u8> = Vec::new();
            let read_size = tmp_arc.read_to_end(&mut buffer).expect("intermediate read failed");
            print!("read_size: {}\n", read_size);
            yield buffer
        }

        zip.finish();

        // Flush zip metadata to listener and close up shop
        let mut buffer: Vec<u8> = Vec::new();
        let read_size = tmp_arc.read_to_end(&mut buffer).expect("final read failed");
        print!("read_size: {}\n", read_size);
        yield buffer
    })
  
}

//===================================== rocket boilerplate ===================================/
use rocket::Request;
use rocket::response::content;

#[catch(404)]
fn not_found(request: &Request<'_>) -> content::RawHtml<String> {
    let html = match request.format() {
        Some(ref mt) if !(mt.is_xml() || mt.is_html()) => {
            format!("<p>'{}' requests are not supported.</p>", mt)
        }
        _ => format!("<p>Sorry, '{}' is an invalid path! Try \
            /stream/images; instead.</p>",
        request.uri())
    };

    content::RawHtml(html)
}

#[launch]
fn rocket() -> _ {
    // As vanilla a rocket as was ever launched
    rocket::build()
        .mount("/", routes![stream_img_files])
        .register("/", catchers![not_found])
}
