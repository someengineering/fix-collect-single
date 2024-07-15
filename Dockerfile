ARG IMAGE_TAG=edge
FROM ghcr.io/someengineering/fixinventorybase:${IMAGE_TAG}

# Install this project into the existing venv
ADD . /single_coordinator
RUN . /usr/local/fix-venv-python3/bin/activate && pip install /single_coordinator && rm -rf /single_coordinator

# Add shim and create symlink
COPY dispatch_executable_shim.sh /usr/local/bin/dispatch_executable_shim
RUN chmod 755 /usr/local/bin/collect_single_shim && ln -s /usr/local/bin/collect_single_shim /usr/bin/collect_single

ENTRYPOINT ["/bin/dumb-init", "--", "/usr/local/sbin/bootstrap", "/usr/bin/dispatch_executable_shim"]
