# Cian Apartment Scraper

A comprehensive scraper for Cian.ru apartment listings with automated image processing using GitHub Actions.

## Features

- 🏠 **Apartment Data Scraping**: Extracts listing details from Cian.ru
- 🖼️ **Image Processing**: Downloads and processes apartment images with ML prediction
- 🤖 **GitHub Actions Integration**: Automated image processing in the cloud
- 📊 **Data Export**: Converts data to CSV format
- 🎯 **Smart Filtering**: Only processes listings that need image updates
- 🧹 **Deduplication**: Removes duplicate images automatically

## Setup

### Prerequisites

- Python 3.9+
- GitHub account
- GitHub Personal Access Token

### Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/cian-parse-page.git
cd cian-parse-page
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your settings
```

### GitHub Actions Setup

1. Create a GitHub Personal Access Token:
   - Go to GitHub Settings → Developer settings → Personal access tokens
   - Generate new token with `repo` and `workflow` scopes

2. Add repository secrets:
   - Go to your repository → Settings → Secrets → Actions
   - Add `CIAN_TRACKER_TOKEN` with your GitHub token

## Usage

### Local Processing
```bash
python scheduler.py --once
```

### GitHub Actions Processing
```bash
export USE_GITHUB_ACTIONS=true
export GITHUB_TOKEN=your_token
python scheduler.py --once
```

### Scheduling
```bash
python scheduler.py --schedule
```

## Configuration

Edit `.env` file:

```bash
# GitHub Actions mode
USE_GITHUB_ACTIONS=true
GITHUB_TOKEN=your_token
GITHUB_REPO_OWNER=your_username
GITHUB_REPO_NAME=cian-parse-page

# Image storage
CIAN_TRACKER_REPO=klimmm/cian-tracker
```

## Architecture

- **scheduler.py**: Main orchestrator
- **cian.py**: Core scraping logic
- **utils/**: Utility functions (filtering, deduplication, etc.)
- **image_utils/**: Image processing and ML prediction
- **scripts/**: Standalone scripts for GitHub Actions
- **.github/workflows/**: GitHub Actions workflows

## GitHub Actions Workflow

The workflow automatically:
1. Filters listings that need image processing
2. Downloads and processes images with ML predictions
3. Deduplicates similar images
4. Commits results to the cian-tracker repository

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License - see LICENSE file for details