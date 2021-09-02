#[macro_use] extern crate rocket;

use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufReader, Write};
use serde::Deserialize;
use zip::ZipWriter;
use zip::write::FileOptions;
use zip::result::ZipResult;

//===================================== support for our endpoint ===================================/

#[derive(Deserialize, Debug)]
struct UrlFile {
    url: String,
    filename: String,
}

fn parse_manifest() -> Result<Vec<UrlFile>, Box<dyn Error>> {
    // Presumably this is a static file that's updated regularly by another process
    let file = File::open("../sample_archive.json")?;

    // Create a buffered reader on it because we can't be sure how big it may grow to be
    let reader = BufReader::new(file);

    // Parse the entire JSON array in one gulp, using typed JSON parser magic 
    // Yes, it could be argued that refactoring this to yield short segments would scale better
    let manifest: Vec<UrlFile> = serde_json::from_reader(reader)?;
    Ok(manifest)
}

async fn fetch_img_file(zip: &mut ZipWriter<File>, options: FileOptions, url: &str, filename: &str) -> ZipResult<()> {
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
async fn stream_img_files() -> io::Result<String> {

    // Load the JSON file 
    let manifest = parse_manifest().expect("problem parsing manifest");

    // Create a local target zip archive, and a ZipWriter to do the heavy lifting
    let archive = File::create("urls_to_zip_v2_image_archive.zip").expect("couldn't create archive file");
    let mut zip = ZipWriter::new(archive);
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    // Iterate over each url:basefilename pair, downloading/compressing/appending on a file-wise basis
    for url_file in manifest.iter() {
        fetch_img_file(&mut zip, options, &url_file.url, &url_file.filename).await.expect("failed to fetch image file");
    };

    // Seal the deal
    zip.finish()?;

    // Conversation piece, left for discussion - rocket crate route function requirement 
    Ok("".to_string())
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

use rocket::response::stream::ByteStream;
use rocket::futures::stream::repeat;
use rocket::tokio::time::{self, Duration};

#[get("/bytes")]
fn bytes() -> ByteStream![&'static [u8]] {
    ByteStream(repeat(&[1, 2, 3][..]))
}

#[get("/byte/stream")]
fn stream() -> ByteStream![Vec<u8>] {
    ByteStream! {
        let mut interval = time::interval(Duration::from_secs(1));
        for i in 0..10u8 {
            yield vec![i, i + 1, i + 2];
            interval.tick().await;
        }
    }
}


#[launch]
fn rocket() -> _ {

    // As vanilla a rocket as was ever launched
    rocket::build()
        .mount("/", routes![stream_img_files])
        .mount("/", routes![bytes, stream])
        .register("/", catchers![not_found])

}
