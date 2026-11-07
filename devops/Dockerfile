FROM ubuntu:latest
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y nginx
COPY index.html .
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]