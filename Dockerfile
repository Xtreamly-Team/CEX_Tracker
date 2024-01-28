FROM python:3.12

COPY . .

RUN pip install -r requirements.txt

# EXPOSE 4343

CMD ["python", "src/realtime.py"]
