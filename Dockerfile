
FROM node:18-alpine AS builder

RUN npm install -g yarn

WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install --frozen-lockfile

COPY . .

RUN yarn build

FROM node:18-alpine AS production

RUN npm install -g yarn

RUN addgroup -g 1001 -S nodejs && \
    adduser -S appuser -u 1001
WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install --frozen-lockfile --production && \
    yarn cache clean


COPY --from=builder /app/dist ./dist

COPY --from=builder /app/src/Planilhas ./src/Planilhas

RUN chown -R appuser:nodejs /app
USER appuser

EXPOSE 3000

ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

CMD ["node", "dist/server.js"]
