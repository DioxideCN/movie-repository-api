FROM node:20.10.0 AS builder

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:20.10.0

WORKDIR /app
COPY --from=builder /app/package.json /app/package.json
COPY --from=builder /app/node_modules /app/node_modules
COPY --from=builder /app/.next /app/.next

COPY --from=builder /app/public /app/public
CMD ["npm", "start"]
