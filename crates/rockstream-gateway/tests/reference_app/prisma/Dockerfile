FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev 2>/dev/null || npm install
COPY . .
CMD ["node", "app.js"]
