FROM node:26.13.0-alpine3.18 AS builder
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY . .
RUN npm run build

# Runner build
FROM node:26.13.0-alpine3.18 AS runner
WORKDIR /app
COPY --from=builder /app/package*.json ./
RUN npm install --production
COPY --from=builder /app/dist ./dist
CMD ["node", "dist/index.js"]