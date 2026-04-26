# Bike Rentals — Apache Spark Analysis

A Java-based data analysis project using **Apache Spark** to explore a bike rental dataset with SQL queries and aggregations.

---

##  Project Structure

```
src/
└── main/
    ├── java/com/
    │   └── Main.java
    └── resources/
        └── data/
            └── data.csv
```

---

##  Dataset

The dataset (`data.csv`) contains **300 rows** of bike rental records with the following columns:

| Column | Type | Description |
|---|---|---|
| `rental_id` | String | Unique ID for each rental |
| `user_id` | String | Unique ID of the user |
| `age` | Integer | User's age |
| `gender` | String | M or F |
| `start_time` | Timestamp | Rental start timestamp |
| `end_time` | Timestamp | Rental end timestamp |
| `start_station` | String | Where the bike was picked up |
| `end_station` | String | Where the bike was returned |
| `duration_minutes` | Integer | Length of the trip in minutes |
| `price` | Double | Rental cost in dollars |

---

##  Prerequisites

- Java **11+**
- Apache Spark **3.x**
- Maven or Gradle
- An IDE (IntelliJ IDEA recommended)

---

##  Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd bike-rentals-spark
   ```

2. **Place the dataset**  
   Copy `data.csv` into `src/main/resources/data/`

3. **Run the project**  
   Execute the `Main` class from your IDE, or build and run via Maven:
   ```bash
   mvn compile exec:java -Dexec.mainClass="com.Main"
   ```

---

## Analyses Performed

### Basic Operations
- Display schema and first 5 rows
- Count total number of rentals
- Filter rentals with `duration_minutes > 30`
- Filter rentals starting from `River Station`
- Compute total revenue (`SUM(price)`)

### Station Aggregations
- Number of rentals per station
- Average trip duration per station
- Most popular station (highest rental count)

### Time Analysis
- Extract hour and minute from `start_time`
- Busiest hours of the day (rentals by hour)
- Most active station during morning hours (7h–12h)

### User Demographics
- Average user age
- Rental count by gender
- Rental distribution by age group:
  - 18–30
  - 31–40
  - 41–50
  - 51+

---

##  How It Works

The app creates a local Spark session, loads the CSV with an explicit schema, registers a temporary SQL view (`bike_rentals_view`), then runs a series of SQL queries using `spark.sql(...)`.

```java
SparkSession spark = SparkSession.builder()
    .appName("city_analysis")
    .master("local[*]")
    .getOrCreate();
```

All queries are executed against the in-memory view and results are printed to the console.

---

##  License

This project is for educational purposes.

## Mohamed SAIFI
## Data Engineer
