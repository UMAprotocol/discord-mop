FROM node:16-slim

RUN apt-get -y update

WORKDIR /discord-mop

COPY . $WORKDIR

RUN npm install pm2 -g && \
    npm install -g typescript 

RUN yarn install && \
    tsc

CMD ["pm2-runtime", "index.js"]
