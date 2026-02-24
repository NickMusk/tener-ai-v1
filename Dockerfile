FROM node:20-alpine AS base

WORKDIR /app

COPY package.json ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

EXPOSE 10000
ENV PORT=10000

CMD ["npm", "start"]
