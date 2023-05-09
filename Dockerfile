
FROM golang:1.20  

RUN apt update

# RUN wget -O- https://apt.releases.hashicorp.com/gpg |  gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg

# RUN echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/hashicorp.list

RUN  apt update
RUN  apt install -y consul

# RUN git clone https://github.com/hashicorp/consul.git
# RUN cd consul
# RUN make dev
# RUN consul version

WORKDIR /quanta

COPY go.mod go.sum ./

RUN go mod download
RUN go mod verify

COPY . ./

RUN cd localCluster; CGO_ENABLED=0 GOOS=linux go build -o /quanta-local-bin

CMD ["./local-start.sh"]

# docker build -t quanta-local .
# docker run -p 4000:4000 -p 8500:8500 quanta-local  

#  ./:/localClusterData