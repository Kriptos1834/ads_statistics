FROM python:3.10.2

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="Europe/Moscow"

RUN apt-get update && apt-get install -y cron nano rsyslog

# setup workdir ----------------------------------
WORKDIR /ads_stat
# setup rsyslog ----------------------------------
RUN echo "cron.*                         /var/log/cron.log" >> /etc/rsyslog.conf

RUN service rsyslog restart
# setup python env -------------------------------
COPY requirements.txt requirements.txt

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt
# setup cron -------------------------------------
RUN echo "#!/bin/bash\ntouch /etc/crontab /etc/cron.d/* /var/spool/cron/crontabs/*" >> /etc/init.d/touch-crond && chmod 744 /etc/init.d/touch-crond

RUN echo "* * * * * /ads_stat/scripts/schedule.sh > /proc/1/fd/1 2>&1" | crontab -u root -
# run container ----------------------------------
EXPOSE 5000

COPY . .

RUN chmod +x scripts/schedule.sh

CMD ["scripts/start.sh"]