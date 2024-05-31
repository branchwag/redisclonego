package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type storeValue struct {
	value  string
	expiry int64
}

var (
	store             = make(map[string]*storeValue)
	lock              = sync.RWMutex{}
	role              = "master"
	masterReplID      = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset  = 0
	replicas          = make([]net.Conn, 0)
	replicaConnections = sync.Mutex{}
)

func parseRESPMessage(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	if line[0] != '*' {
		return nil, fmt.Errorf("expected '*', got '%c'", line[0])
	}

	numArgs, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)

		if line[0] != '$' {
			return nil, fmt.Errorf("expected '$', got '%c'", line[0])
		}

		argLen, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}

		arg := make([]byte, argLen+2)
		_, err = reader.Read(arg)
		if err != nil {
			return nil, err
		}

		args[i] = string(arg[:argLen])
	}

	return args, nil
}

func handleConnections(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	emptyRDB := []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
		0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
		0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
		0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
		0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0xc0, 0x00, 0xff,
		0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
	}

	for {
		args, err := parseRESPMessage(reader)
		if err != nil {
			fmt.Println("Error parsing message: ", err.Error())
			return
		}

		if len(args) > 0 {
			command := strings.ToUpper(args[0])

			switch command {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(args) > 1 {
					message := args[1]
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)))
				}
			case "SET":
				if len(args) >= 3 {
					key, value := args[1], args[2]
					expiry := int64(0)
					if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
						if px, err := strconv.Atoi(args[4]); err == nil {
							expiry = time.Now().Add(time.Duration(px) * time.Millisecond).UnixMilli()
						}
					}
					lock.Lock()
					store[key] = &storeValue{value: value, expiry: expiry}
					lock.Unlock()
					masterReplOffset += len(args[1]) + len(args[2])
					conn.Write([]byte("+OK\r\n"))
					propagateToReplicas(args)
				}
			case "GET":
				if len(args) > 1 {
					key := args[1]
					lock.RLock()
					if val, exists := store[key]; exists && (val.expiry == 0 || val.expiry > time.Now().UnixMilli()) {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
					lock.RUnlock()
				}
			case "DEL":
				if len(args) > 1 {
					key := args[1]
					lock.Lock()
					delete(store, key)
					lock.Unlock()
					masterReplOffset += len(args[1])
					conn.Write([]byte(":1\r\n"))
					propagateToReplicas(args)
				}
			case "INFO":
				if len(args) == 2 && strings.ToUpper(args[1]) == "REPLICATION" {
					info := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n", role, masterReplID, masterReplOffset)
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
				}
			case "REPLCONF":
				conn.Write([]byte("+OK\r\n"))
			case "PSYNC":
				if len(args) == 3 && args[1] == "?" && args[2] == "-1" {
					response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", masterReplID)
					conn.Write([]byte(response))

					rdbLen := len(emptyRDB)
					conn.Write([]byte(fmt.Sprintf("$%d\r\n", rdbLen)))
					conn.Write(emptyRDB)

					replicaConnections.Lock()
					replicas = append(replicas, conn)
					replicaConnections.Unlock()
				}
			}
		}
	}
}

func connectToMaster(masterHost string, masterPort int, replicaPort int) {
	address := fmt.Sprintf("%s:%d", masterHost, masterPort)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect to master at %s\n", address)
		return
	}
	defer conn.Close()

	readResponse := func(conn net.Conn, expectedResp string) error {
		buf := make([]byte, len(expectedResp))
		_, err := conn.Read(buf)
		if err != nil {
			return err
		}
		if string(buf) != expectedResp {
			return fmt.Errorf("unexpected response from master: got %s, want %s", string(buf), expectedResp)
		}
		return nil
	}

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	_, err = conn.Write([]byte(pingCommand))
	if err != nil {
		fmt.Printf("Failed to send PING to master: %s\n", err.Error())
		return
	}
	fmt.Println("Sent PING to master")

	err = readResponse(conn, "+PONG\r\n")
	if err != nil {
		fmt.Printf("Failed to read PING response from master: %s\n", err.Error())
		return
	}
	fmt.Println("Received PING response from master")

	replConfListeningPort := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n", len(strconv.Itoa(replicaPort)), replicaPort)
	_, err = conn.Write([]byte(replConfListeningPort))
	if err != nil {
		fmt.Printf("Failed to send REPLCONF listening-port to master: %s\n", err.Error())
		return
	}
	fmt.Println("Sent REPLCONF listening-port to master")

	err = readResponse(conn, "+OK\r\n")
	if err != nil {
		fmt.Printf("Failed to read REPLCONF listening-port response from master: %s\n", err.Error())
		return
	}
	fmt.Println("Received REPLCONF listening-port response from master")

	replConfCapaPsync2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	_, err = conn.Write([]byte(replConfCapaPsync2))
	if err != nil {
		fmt.Printf("Failed to send REPLCONF capa psync2 to master: %s\n", err.Error())
		return
	}
	fmt.Println("Sent REPLCONF capa psync2 to master")

	err = readResponse(conn, "+OK\r\n")
	if err != nil {
		fmt.Printf("Failed to read REPLCONF capa psync2 response from master: %s\n", err.Error())
		return
	}
	fmt.Println("Received REPLCONF capa psync2 response from master")

	psyncCommand := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err = conn.Write([]byte(psyncCommand))
	if err != nil {
		fmt.Printf("Failed to send PSYNC to master: %s\n", err.Error())
		return
	}
	fmt.Println("Sent PSYNC to master")

	respPsync := make([]byte, 50)
	_, err = conn.Read(respPsync)
	if err != nil {
		fmt.Printf("Failed to read PSYNC response from master: %s\n", err.Error())
		return
	}
	fmt.Println("Received PSYNC response from master:", string(respPsync))

	fmt.Println("Handshake completed successfully")

	// Process commands from master
	reader := bufio.NewReader(conn)
	for {
		args, err := parseRESPMessage(reader)
		if err != nil {
			fmt.Println("Error reading from master: ", err.Error())
			return
		}
		if len(args) > 0 {
			processCommand(args)
		}
	}
}

func processCommand(args []string) {
	if len(args) > 0 {
		command := strings.ToUpper(args[0])

		switch command {
		case "SET":
			if len(args) >= 3 {
				key, value := args[1], args[2]
				expiry := int64(0)
				if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
					if px, err := strconv.Atoi(args[4]); err == nil {
						expiry = time.Now().Add(time.Duration(px) * time.Millisecond).UnixMilli()
					}
				}
				lock.Lock()
				store[key] = &storeValue{value: value, expiry: expiry}
				lock.Unlock()
			}
		case "DEL":
			if len(args) > 1 {
				key := args[1]
				lock.Lock()
				delete(store, key)
				lock.Unlock()
			}
		}
	}
}

func propagateToReplicas(args []string) {
	replicaConnections.Lock()
	defer replicaConnections.Unlock()

	command := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		command += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	for _, replica := range replicas {
		_, err := replica.Write([]byte(command))
		if err != nil {
			fmt.Printf("Failed to send command to replica: %s\n", err.Error())
		}
	}
}

func main() {
	port := flag.Int("port", 6379, "Port to run the Redis server on")
	replicaOf := flag.String("replicaof", "", "Master host and port in the format 'host port' for replication")
	flag.Parse()

	if *replicaOf != "" {
		role = "slave"
	}

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	fmt.Printf("Starting Redis server on %s with role %s\n", address, role)

	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *port)
		os.Exit(1)
	}
	defer l.Close()

	if *replicaOf != "" {
		parts := strings.Split(*replicaOf, " ")
		if len(parts) == 2 {
			masterHost := parts[0]
			masterPort, err := strconv.Atoi(parts[1])
			if err == nil {
				go connectToMaster(masterHost, masterPort, *port)
			}
		}
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnections(conn)
	}
}
