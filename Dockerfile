ARG IMAGE_TAG=latest
FROM ghcr.io/someengineering/resotobase:${IMAGE_TAG}

# Install this project into the existing venv
ADD . /single_coordinator
RUN . /usr/local/resoto-venv-python3/bin/activate && pip install --no-deps /single_coordinator && rm -rf /single_coordinator

# Add shim and create symlink
COPY collect_single_shim /usr/local/bin/collect_single_shim
RUN chmod 755 /usr/local/bin/collect_single_shim && ln -s /usr/local/bin/collect_single_shim /usr/bin/collect_single

ENTRYPOINT ["/bin/dumb-init", "--", "/usr/local/sbin/bootstrap", "/usr/bin/collect_single"]
