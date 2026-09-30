# TripWire: Distributed Price Ticker

TripWire is a distributed engineering project designed to provide real-time, aggregated price feeds for stocks and cryptocurrencies. It leverages a multi-source ingestion pipeline to ensure data accuracy and availability, making it resilient to individual API failures or anomalies.

## 🚀 Features
- **Multi-Source Ingestion**: Fetches data from various APIs (Alpha Vantage, Finnhub, Polygon, CoinGecko, Binance, CMC).
- **Real-time Updates**: Low-latency price broadcasting using WebSockets.
- **Distributed Architecture**: Decoupled ingestion and processing layers via Apache Kafka.
- **Fast Storage & Retrieval**: Redis for latest price caching and time-series history.
- **Data Validation**: Built-in outlier detection and confidence scoring.

## 🏗 Architecture & Data Flow

The system follows a pipeline architecture to move data from external APIs to the end user:

1. **Ingestion Layer**: 
   - `StockPriceIngestorService` and `CryptoPriceIngestorService` poll external APIs asynchronously using `CompletableFuture`.
   - Prices from multiple sources for the same symbol are aggregated.
2. **Messaging Layer**: 
   - The aggregated prices are published to specific Kafka topics (`stock-prices`, `crypto-prices`).
3. **Processing Layer**: 
   - `PriceProcessorService` consumes messages from Kafka.
   - It updates the current price in Redis and appends the update to a historical sorted set.
4. **Delivery Layer**: 
   - The processed update is broadcasted via Spring WebSockets to all subscribed clients in real-time.

## 🧠 Algorithms

### Outlier Removal
To prevent a single malfunctioning API from skewing the price, TripWire implements an outlier detection algorithm in `PriceAggregatorService`:
- **Logic**: While the difference between the maximum and minimum price in a set is greater than 5%, the lowest value is removed.
- **Goal**: Ensures the final average is derived from a consistent cluster of data points.

### Confidence Scoring
The system calculates a confidence score for every aggregated price:
- **Calculation**: Based on the average deviation of all filtered sources from the mean.
- **Result**: A score from 0.0 to 1.0. High consistency across APIs results in a score closer to 1.0; high variance results in a lower score.

## 🛠 Local Setup

### Prerequisites
- **Java 17**
- **Maven**
- **Docker & Docker Compose**

### Running the Project

1. **Start Infrastructure**:
   Launch Kafka, Zookeeper, and Redis using Docker Compose.
   ```bash
   docker-compose up -d
   ```

2. **Configure API Keys**:
   Update `src/main/resources/application.properties` with your API keys for Alpha Vantage, Finnhub, Polygon, and CoinMarketCap.

3. **Build and Run**:
   ```bash
   ./mvnw clean install
   ./mvnw spring-boot:run
   ```

## 📡 API & Endpoints

### WebSockets
Subscribe to these topics to receive real-time updates:
- **Specific Stock**: `/topic/prices/stock/{symbol}`
- **Specific Crypto**: `/topic/prices/crypto/{symbol}`
- **All Updates**: `/topic/prices/all`

### Health Checks
- `GET /health`: Check the overall system health and API connectivity.
