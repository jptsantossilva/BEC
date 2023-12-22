FROM python:3.10.6 as base

# set working directory
WORKDIR /BEC

# install dependencies
COPY ./requirements.txt /BEC
RUN pip install --no-cache-dir -r requirements.txt

# copy files to the folder
COPY . /BEC

CMD ["python3","main.py","1h","test"]
