FROM python:3.9

RUN pip install virtualenv
ENV VIRTUAL_ENV=/venv
RUN virtualenv venv -p python3
ENV PATH="VIRTUAL_ENV/bin:$PATH"

WORKDIR /usr/src/app
ADD . /usr/src/app

# Install dependencies
RUN pip install -r requirements.txt

# copying all files over
COPY . /usr/src/app

# Expose port 
ENV PORT 8501

# cmd to launch app when container is run
CMD streamlit run app.py

# streamlit-specific commands for config
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN mkdir -p /root/.streamlit
RUN bash -c 'echo -e "\
    [general]\n\
    email = \"\"\n\
    " > /root/.streamlit/credentials.toml'

RUN bash -c 'echo -e "\
    [server]\n\
    enableCORS = false\n\
    " > /root/.streamlit/config.toml'