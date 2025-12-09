use anyhow::Result;
use chronos::parser::Value;
use chronos::storage::{
    create_storage_engine, Column, DataType, Filter, FilterOp, Row, StorageConfig, TableSchema,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("Chronos Storage Engine Demo\n");

    let config = StorageConfig::Sled {
        data_dir: "./demo_data".to_string(),
    };

    let mut engine = create_storage_engine(config)?;
    engine.init().await?;

    println!("Storage engine initialized\n");

    let schema = TableSchema {
        name: "iot_sensors".to_string(),
        columns: vec![
            Column {
                name: "device_id".to_string(),
                data_type: DataType::String,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "temperature".to_string(),
                data_type: DataType::Float,
            },
            Column {
                name: "humidity".to_string(),
                data_type: DataType::Float,
            },
            Column {
                name: "location".to_string(),
                data_type: DataType::String,
            },
        ],
        ttl_seconds: None,
    };

    engine.create_table("iot_sensors", schema).await?;
    println!("Created table 'iot_sensors'\n");

    let sensor_data = vec![
        create_sensor_row("SENSOR_001", 1700000000, 22.5, 65.0, "Factory_Floor_A"),
        create_sensor_row("SENSOR_002", 1700000001, 23.1, 62.5, "Factory_Floor_A"),
        create_sensor_row("SENSOR_003", 1700000002, 21.8, 68.0, "Factory_Floor_B"),
        create_sensor_row("SENSOR_001", 1700000003, 22.9, 64.5, "Factory_Floor_A"),
        create_sensor_row("SENSOR_004", 1700000004, 25.2, 70.0, "Warehouse"),
    ];

    engine.insert("iot_sensors", sensor_data).await?;
    println!("Inserted 5 sensor readings\n");

    println!("All sensor readings:");
    let all_data = engine.query("iot_sensors", None).await?;
    print_sensor_data(&all_data);

    println!("\nSensors with temperature > 23.0 C:");
    let filter = Filter {
        column: "temperature".to_string(),
        op: FilterOp::Gt,
        value: Value::Float(23.0),
    };
    let filtered_data = engine.query("iot_sensors", Some(filter)).await?;
    print_sensor_data(&filtered_data);

    engine.create_index("iot_sensors", "device_id").await?;
    println!("\nCreated index on 'device_id'\n");

    engine.checkpoint().await?;
    println!("Data checkpointed to disk\n");

    let tables = engine.list_tables().await?;
    println!("Tables in database: {:?}\n", tables);

    engine.close().await?;
    println!("Storage engine closed");

    Ok(())
}

fn create_sensor_row(
    device_id: &str,
    timestamp: i64,
    temperature: f64,
    humidity: f64,
    location: &str,
) -> Row {
    let mut row = Row::new();
    row.insert(
        "device_id".to_string(),
        Value::String(device_id.to_string()),
    );
    row.insert("timestamp".to_string(), Value::Integer(timestamp));
    row.insert("temperature".to_string(), Value::Float(temperature));
    row.insert("humidity".to_string(), Value::Float(humidity));
    row.insert("location".to_string(), Value::String(location.to_string()));
    row
}

fn print_sensor_data(rows: &[Row]) {
    for row in rows {
        println!(
            "  Device: {:?}, Temp: {:?} C, Humidity: {:?}%, Location: {:?}",
            row.get("device_id"),
            row.get("temperature"),
            row.get("humidity"),
            row.get("location")
        );
    }
}
