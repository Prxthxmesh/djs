FROM node:20-alpine AS server-build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json ./
COPY src ./src
RUN npm run build:server && mkdir -p dist/migrations && cp -r src/migrations/* dist/migrations/

FROM node:20-alpine AS web-build
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web ./
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY --from=server-build /app/dist ./dist
COPY --from=web-build /app/web/dist ./web/dist
RUN mkdir -p /app/benchmarks && chown -R node:node /app/benchmarks
USER node
EXPOSE 3000 9101
CMD ["node", "dist/services/api-main.js"]
