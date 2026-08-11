FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

# One-shot: create the topics, then exit.
CMD ["node", "admin.js"]
