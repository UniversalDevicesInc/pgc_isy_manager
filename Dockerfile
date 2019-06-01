FROM node:11-alpine

ARG STAGE=test
ENV STAGE $STAGE

RUN mkdir -p /app/

WORKDIR /app
COPY package.json isy_manager.js /app/

RUN npm install
ENTRYPOINT [ "node", "isy_manager.js"]
