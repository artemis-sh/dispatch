# syntax=docker/dockerfile:1.7

ARG NODE_VERSION=24
ARG OPENCODE_VERSION=1.18.13

FROM node:${NODE_VERSION}-slim
ARG OPENCODE_VERSION
ENV NODE_ENV=production
WORKDIR /workspace
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates git \
  && rm -rf /var/lib/apt/lists/* \
  && npm install -g opencode-ai@${OPENCODE_VERSION} \
  && npm cache clean --force \
  && chown -R node:node /workspace
COPY --chmod=0555 sandbox-image/git-workspace-materializer.mjs /opt/dispatch/git-workspace-materializer.mjs
USER node
EXPOSE 4096
ENTRYPOINT ["node", "/opt/dispatch/git-workspace-materializer.mjs"]
CMD ["opencode", "serve", "--hostname", "0.0.0.0", "--port", "4096"]
