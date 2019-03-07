package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

type Message struct {
	Msg string
}

type ReplyValue struct {
	Value string
}

type KeyValue struct {
	Key   string
	Value string
}

type Server struct {
	kv          map[string]string // mock k/v map
	machines    []string          // mock AWS instances
	machinesMap map[string]bool
	alive       map[string]bool
	router      *mux.Router
}

func main() {
	// initClerk()
	startServer()
}

// TODO: use kv service
// func initClerk() {
// 	ends := make([]*labrpc.ClientEnd, 5)
// 	ck := kvraft.MakeClerk(ends)
// 	fmt.Println(ck)
// }

func startServer() {
	router := mux.NewRouter().StrictSlash(true)
	sv := Server{
		kv:          make(map[string]string),
		machines:    []string{"A", "B", "C", "D", "E"},
		machinesMap: map[string]bool{"A": true, "B": true, "C": true, "D": true, "E": true},
		alive:       map[string]bool{"A": true, "B": true, "C": true, "D": true, "E": true},
		router:      router,
	}

	router.HandleFunc("/state", sv.getState).Methods("GET").Queries("key", "{key}")
	router.HandleFunc("/state", sv.setState).Methods("POST")
	router.HandleFunc("/machines/all", sv.getMachineByAction("all")).Methods("GET")
	router.HandleFunc("/machines/alive", sv.getMachineByAction("alive")).Methods("GET")
	router.HandleFunc("/machine/{id}", sv.setMachine).Methods("DELETE")
	router.HandleFunc("/machine/{id}", sv.setMachine).Methods("POST")

	// Enable CORS
	originsOk := handlers.AllowedOrigins([]string{"*"})
	headersOk := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "POST", "DELETE", "OPTIONS"})

	log.Fatal(http.ListenAndServe(":8080", handlers.CORS(originsOk, headersOk, methodsOk)(router)))
}

func (sv *Server) getState(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	key := params["key"]
	value, ok := sv.kv[key]
	if ok {
		json.NewEncoder(w).Encode(ReplyValue{value})
	} else {
		json.NewEncoder(w).Encode(ReplyValue{""})
	}
}

func (sv *Server) setState(w http.ResponseWriter, r *http.Request) {
	var data KeyValue
	err := json.NewDecoder(r.Body).Decode(&data)

	if err != nil {
		json.NewEncoder(w).Encode(Message{"Error decoding"})
	} else {
		sv.kv[data.Key] = data.Value
		json.NewEncoder(w).Encode(Message{"Done"})
	}
}

func (sv *Server) getMachineByAction(category string) func(http.ResponseWriter, *http.Request) {
	switch category {
	case "all":
		return func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(sv.machines)
		}
	case "alive":
		return func(w http.ResponseWriter, r *http.Request) {
			alive := make([]string, 0, len(sv.machines))
			for _, machine := range sv.machines {
				if isAlive := sv.alive[machine]; isAlive {
					alive = append(alive, machine)
				}
			}
			json.NewEncoder(w).Encode(alive)
		}
	default:
		return func(w http.ResponseWriter, r *http.Request) {}
	}
}

func (sv *Server) setMachine(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	if exist := sv.machinesMap[id]; exist {
		if r.Method == "DELETE" {
			sv.alive[id] = false
		} else if r.Method == "POST" {
			sv.alive[id] = true
		}
	}
	json.NewEncoder(w).Encode(Message{"Done"})
}
