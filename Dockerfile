FROM python:3.11

# Create a folder to store our code
RUN mkdir /app

# Set the folder as our working directory
WORKDIR /app

# Copy the pip dependencies file
COPY requirements.txt ./

# Copy all .py files
COPY *.py ./

# Copy the dataset
COPY dataset.txt ./

# Install project dependencies
RUN pip install -r requirements.txt

CMD ["python", "main.py"]