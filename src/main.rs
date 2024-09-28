use csv::Writer;
use geo::{Area, Geometry};
use geojson::GeoJson;
use rayon::prelude::*; // 並列処理用
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    sync::{Arc, Mutex},
    time::Instant,
};

/**
 * GeoJSON ファイルを読み込んで、市町村ごとの面積を集計して CSV に出力する。
 * GeoJSON ファイルは、国土数値情報の「行政区域データ」を利用。
 * https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N03-v2_3.html
 */
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    // GeoJSON を読み込む
    let file = File::open("src/N03-20240101_11.geojson")?;
    let reader = BufReader::new(file);
    let geojson: GeoJson = GeoJson::from_reader(reader)?;

    // 集計用の HashMap を Arc と Mutex でラップ（都道府県名 -> 面積）
    // Arc は複数のスレッドから所有権を共有して参照できるようにするためのスマートポインタ
    // Mutex は複数のスレッドから安全にデータにアクセスするための同期プリミティブ
    let area_map = Arc::new(Mutex::new(HashMap::<String, f64>::new()));

    // GeoJSON の FeatureCollection から Feature を取り出す
    if let GeoJson::FeatureCollection(collection) = geojson {
        // 各 Feature を並列に処理
        collection.features.par_iter().for_each(|feature| {
            if let Some(geometry) = &feature.geometry {
                let result: Result<Geometry<f64>, _> = geometry.value.clone().try_into();
                match result {
                    Ok(geo_geometry) => {
                        let area = geo_geometry.unsigned_area();

                        // 市町村名を取得して面積を集計
                        if let Some(properties) = &feature.properties {
                            if let Some(city_name) = properties.get("N03_004") {
                                if let Some(city_name_str) = city_name.as_str() {
                                    // 面積を集計（スレッドセーフに更新）
                                    let mut map = area_map.lock().unwrap();
                                    *map.entry(city_name_str.to_string()).or_insert(0.0) += area;
                                }
                            }
                        }
                    }
                    Err(err) => println!("Error: {}", err),
                }
            }
        });
    }

    // Mutexから取り出し、ベクターに変換して面積でソートする
    // HashMap は順序が保証されていないため、Vec に変換してソートする
    let mut sorted_areas: Vec<(String, f64)> = {
        // area_mapのロックを解いてアクセス
        let map = area_map.lock().unwrap();
        map.iter().map(|(k, &v)| (k.clone(), v)).collect()
    };

    // 面積で降順にソート
    sorted_areas.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // CSV に出力する
    let mut wtr = Writer::from_path("output.csv")?;
    wtr.write_record(&["City", "Area"])?;

    for (city, area) in sorted_areas {
        // 算出される面積は正確ではないが、並列処理の勉強用なので許容
        wtr.write_record(&[city, area.to_string()])?;
    }

    wtr.flush()?;
    println!("CSV ファイルに出力しました。");

    // 処理時間を表示
    // 並列に処理した場合、直列処理よりもパフォーマンスが向上したことを確認
    // Node.js で同様の処理を行った場合に比べ、Rust は高速であることがわかった。(Rust: 約30ms, Node.js: 約90ms)
    let end = start.elapsed();
    println!("処理時間: {}.{:03} 秒", end.as_secs(), end.subsec_millis());

    Ok(())
}
