# PDF → Customer Email Matcher (Flask)

Upload a **Products PDF or CSV** + **Customers CSV**, get a personalized **mail_merge.csv** for bulk emailing.

## Run locally
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
# open http://localhost:8000
