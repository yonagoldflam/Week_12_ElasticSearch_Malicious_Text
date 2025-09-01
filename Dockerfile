FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt
ENV NLTK_DATA=/usr/local/share/nltk_data
RUN python -m nltk.downloader -d /usr/local/share/nltk_data vader_lexicon

COPY data ./data
COPY data_loader ./data_loader
COPY elastic ./elastic

EXPOSE 8000

CMD ["uvicorn", "elastic.main:app", "--host", "0.0.0.0", "--port", "8000"]