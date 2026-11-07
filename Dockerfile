# Install all dependencies and compile the NestJS application.
FROM node:22-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# Keep the runtime image small by installing production dependencies only.
FROM node:22-alpine AS runner
ENV NODE_ENV=production
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Run the compiled entry point as the unprivileged Node.js user.
COPY --from=builder --chown=node:node /app/dist ./dist
USER node

EXPOSE 3089
CMD ["node", "dist/main.js"]
