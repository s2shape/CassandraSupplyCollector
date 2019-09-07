FROM cassandra
MAINTAINER burilovmv@gmail.com

ADD data.sql docker-entrypoint-initdb.d/

COPY docker-entrypoint2.sh /docker-entrypoint2.sh
RUN ls -la *.sh; chmod +x *.sh; ls -la *.sh

EXPOSE 7000 7001 7199 9042 9160
ENTRYPOINT ["/docker-entrypoint2.sh"]

