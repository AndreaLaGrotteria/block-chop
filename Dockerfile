FROM rust:1.69 as builder

COPY ./chop-chop /chop-chop
COPY ./system-info /chop-chop
WORKDIR /chop-chop

RUN CHOP_CHOP_MESSAGE_SIZE=8 cargo build --release --all-features

FROM ethereum/client-go

ARG NUM-SERVER
ARG ID-SERVER
ARG ETH-PARAM

COPY --from-builder /chop-chop/target /target
RUN mkdir node
WORKDIR /node
COPY ./node/genesis.json /node/genesis.json
COPY ./node/keystore /node/keystore
COPY ./node/password.txt node/password.txt
RUN geth init --datadir . genesis.json

CMD geth --datadir . --bootnodes enode://f9e5e77f2ce5848d6fea4852b7ce0b657c312cf03578393fe79c9d0d4f491775b35d7e307de4ec903c1bae2ba7a9e466ebc698786e3b1d9451ada84365d0dce4@172.31.38.220:30303 \
    --networkid 121212 --unlock 0x66f4B9340e31ef251EF82f7842827d947b16dAc4 --password password.txt && \
    ./target/release/server 127.0.0.1:1234 ./membership.db  $NUM-SERVER $ID-SERVER ./passepartout.db ./directory.db --blockchain $ETH-PARAM --random --heartbeat-path ./heartbeat_server.bin



