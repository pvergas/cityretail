FROM python:3.11-slim

WORKDIR /code

# Copy requirements.txt from the root folder into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything else into the container
COPY . .

# Run your main.py
CMD ["python", "src/main.py"]


