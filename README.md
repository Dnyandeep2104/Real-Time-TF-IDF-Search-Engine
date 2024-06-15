# Real-Time-TF-IDF-Search-Engine

A real-time search engine implemented using Apache Spark and Flask, integrated with AWS S3 for data storage and AWS DynamoDB for TF-IDF data handling.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project implements a real-time search engine that computes TF-IDF scores for document relevance using Apache Spark for data processing and Flask for serving user queries. Data is stored in AWS S3, and TF-IDF metadata is managed in AWS DynamoDB. The system allows users to search through indexed documents and retrieves relevant results based on query terms.

## Features

- **Real-Time Search:** Computes TF-IDF scores dynamically to rank documents based on query relevance.
- **Data Ingestion:** Spark-based pipeline to process text corpora from AWS S3.
- **Dynamic Query Interface:** Flask application to handle user queries and display relevant documents.
- **Integration with AWS Services:** Uses S3 for data storage and DynamoDB for TF-IDF metadata storage.

## Technologies Used

- Apache Spark
- Flask
- AWS S3
- AWS DynamoDB
- Python
- Boto3

## Installation

### Prerequisites

- Python 3.6+
- Apache Spark (configured on an AWS EMR cluster)
- AWS Account with S3 and DynamoDB setup
- Boto3 library (`pip install boto3`)

### Setup Instructions

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/real-time-tfidf-search-engine.git
   cd real-time-tfidf-search-engine
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure AWS Credentials:**

   Ensure your AWS credentials are set up either through environment variables or `~/.aws/credentials` file.

4. **Run the Spark Job (Data Ingestion):**

   Modify project.ipynb to specify your S3 input path and DynamoDB table. Run the job:

   ```bash
   spark-submit spark_job.py
   ```

5. **Start the Flask Application:**

   Update `config.py` with your DynamoDB table name. Start the Flask server:

   ```bash
   python main.py
   ```

6. **Access the Search Interface:**

   Open your web browser and go to `http://localhost:5000` to access the search engine interface.

## Usage

1. **Search Interface:**

   - Enter a query term in the search bar and press Enter.
   - Relevant documents based on TF-IDF scores will be displayed.

2. **Adding New Documents:**

   - Upload new documents to your configured AWS S3 bucket.
   - Re-run the Spark job to update TF-IDF scores in DynamoDB.

## Project Structure

```
real-time-tfidf-search-engine/
│
├── main.py            # Flask application for search interface
├── spark_job.py       # Apache Spark job for data ingestion and TF-IDF computation
├── config.py          # Configuration file (DynamoDB table name, etc.)
├── requirements.txt   # Python dependencies
├── README.md          # Project overview and instructions
└── .gitignore         # Git ignore file
```

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
