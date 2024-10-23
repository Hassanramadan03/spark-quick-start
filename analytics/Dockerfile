From node:18.12.0-alpine
WORKDIR /src/app
ARG PORT
ENV PORT=${PORT}
RUN npm i -g @nestjs/cli
COPY . .
COPY package.json .
RUN npm i 
CMD [ "npm", "run", "start:dev"]
EXPOSE ${PORT}