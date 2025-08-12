
FROM node:20-alpine

RUN apk add --no-cache \
    curl \
    bash

WORKDIR /app

COPY package.json yarn.lock ./

RUN yarn install --frozen-lockfile

COPY . .

RUN yarn build

RUN mkdir -p dist/Planilhas && cp -r src/Planilhas/* dist/Planilhas/ || true

RUN yarn install --frozen-lockfile --production && \
    yarn cache clean

RUN addgroup -g 1001 -S nodejs && \
    adduser -S nextjs -u 1001

RUN chown -R nextjs:nodejs /app
USER nextjs

EXPOSE 3000

# Variáveis de ambiente
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

CMD ["yarn", "start"]
