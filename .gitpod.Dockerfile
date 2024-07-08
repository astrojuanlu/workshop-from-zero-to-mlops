FROM gitpod/workspace-python-3.11:2024-03-20-07-19-19

RUN pyenv global 3.11.8 && pip install uv

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && sudo mv mc /usr/local/bin/mc
