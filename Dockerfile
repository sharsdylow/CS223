# Use the official Python 3.11 image as the base image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY *.py /app/
COPY run_test.sh /app

# Install psycopg library with pip
RUN pip install psycopg

# Set the entrypoint to execute the Python script
ENTRYPOINT ["sh", "-c", "sleep 10 && bash run_test.sh"]
# ENTRYPOINT ["bash", "run_test.sh"]
# CMD ["sleep", "infinity"]
