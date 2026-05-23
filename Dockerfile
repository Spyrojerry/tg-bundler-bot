FROM node:22-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci
RUN npm install -g gmgn-cli

COPY . .
RUN npm run build

ENV NODE_ENV=production
EXPOSE 8080

CMD ["npm", "start"]
