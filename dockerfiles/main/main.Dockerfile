FROM python:3.11.4

RUN mkdir /run/secrets

RUN --mount=type=secret,id=POSTGRES_USER
RUN --mount=type=secret,id=POSTGRES_PASSWORD
RUN --mount=type=secret,id=PGHOST
RUN --mount=type=secret,id=PGPORT
RUN --mount=type=secret,id=PGDATABASE

ENV POSTGRES_USER=/run/secrets/POSTGRES_USER
ENV POSTGRES_PASSWORD=/run/secrets/POSTGRES_PASSWORD

ENV PGUSER=/run/secrets/POSTGRES_USER
ENV PGPASSWORD=/run/secrets/POSTGRES_PASSWORD
ENV PGHOST=/run/secrets/PGHOST
ENV PGPORT=/run/secrets/PGPORT
ENV PGDATABASE=/run/secrets/PGDATABASE

ENV POETRY_VERSION=1.8.2
ENV POETRY_HOME=/usr/local
ENV POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update && \
    apt-get install -y --no-install-recommends

###################

# Install Chrome, ChromeDriver, and dependencies
#RUN apt-get update && apt-get install -y \
#    wget \
#    curl \
#    unzip \
#    xvfb \
#    libxi6 \
#    libgconf-2-4 \
#    default-jdk \
#    && rm -rf /var/lib/apt/lists/*
#
#RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
#    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
#    apt-get update && \
#    apt-get install -y google-chrome-stable
#
#RUN CHROME_DRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE` && \
#    wget -N https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip -P ~ && \
#    unzip ~/chromedriver_linux64.zip -d ~ && \
#    rm ~/chromedriver_linux64.zip && \
#    mv -f ~/chromedriver /usr/local/bin/chromedriver && \
#    chown root:root /usr/local/bin/chromedriver && \
#    chmod 0755 /usr/local/bin/chromedriver

###################

COPY /sql/init.sql /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init.sql

RUN curl -sSL https://install.python-poetry.org | python3 - --version=$POETRY_VERSION

WORKDIR /app

RUN python3 -m venv /venv

COPY ../../poetry.lock pyproject.toml ./

RUN poetry install --no-root

COPY ../.. .

EXPOSE 5432

CMD ["poetry", "run", "python3", "main.py"]