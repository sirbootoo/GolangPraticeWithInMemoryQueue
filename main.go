package main

import (
  "net/http"
  "fmt"
  "github.com/gin-gonic/gin"
  "github.com/google/uuid"
  "time"
)

// A list of task types.
const (
    VerifyUserMSG   = "user:verify"
    CompleteTXMSG     = "tx:complete"
)

type User struct {
	UserId string `json:"userId"`
	Name string `json:"name"`
	IsVerified bool `json:"isVerified"`
	Balance int `json:"balance,omitempty"`
	DontVerify bool `json:"dontVerify"`
}
type Transaction struct {
	Id string `json:"id"` 
	SenderId string `json:"senderId"`
	ReceiverId string `json:"receiverId"`
	Amount int `json:"amount"`
	Status string `json:"status"` // status - pending, failed, success
	Reason string `json:"reason"`
}
type Wallet struct {
	Id string `json:"id"`
	Balance int `json:"balance"`
}
type objDB struct {
	users []User
	transactions []Transaction
}

type Job struct {
	Action string
	Id string
}

var UsersList = make(map[string]*User)
var TransactionsList =  make(map[string]*Transaction)
var UsersWallet =  make(map[string]*Wallet)


var jobs chan Job = make(chan Job, 5) // Create a buffered channel to hold up to 5 jobs
var done chan bool = make(chan bool)



var runtimeInSecond time.Duration = 30
var numberOfWorkers int = 3


func main() {
	r := gin.Default()

	// Create three worker goroutines
	for i := 1; i <= numberOfWorkers; i++ {
		go func(id int) {
			for job := range jobs {
				fmt.Printf("Worker %d started job %s\n", id, job.Id)
				time.Sleep(time.Second)
				fmt.Printf("Worker %d finished job %s\n", id, job.Id)
				worker(job)
			}
		}(i)
	}

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
		"message": "pong",
		})
	});
	// This endpoint will create a user and add the user to a queue
	r.POST("/users", func(c *gin.Context) {
		var userInfo *User
		c.BindJSON(&userInfo)
		// generate a token
		id := uuid.New().String()
		userInfo.UserId = id

		userInfo.IsVerified = false

		UsersList[id] = userInfo
		initialBalance := 0
		if !userInfo.DontVerify {
			initialBalance = 2000
		}
		wallet := &Wallet{
			Id:  uuid.New().String(),
			Balance: initialBalance,
		}
		UsersWallet[id] = wallet
		// send to queue to process
		enQueue(Job{
			Action: "user",
			Id: id,
		})
		c.JSON(http.StatusOK, gin.H{
			"message": "success",
			"data": userInfo,
		})
	})
	r.POST("/tx", func(c *gin.Context) {
		var txInfo *Transaction
		c.BindJSON(&txInfo)
		// generate a token
		id := uuid.New().String()
		txInfo.Id = id
		txInfo.Status = "pending"

		TransactionsList[id] = txInfo
		// send to queue to process
		enQueue(Job{
			Action: "tx",
			Id: id,
		})
		c.JSON(http.StatusOK, gin.H{
			"message": "success",
			"data": txInfo,
		})
	})
	r.GET("/users", func(c *gin.Context) {
		// List users
		for key, element := range UsersList {
			fmt.Println("Key:", key, "=>", "Element:", element)
			wallet := UsersWallet[key]
			UsersList[key].Balance = wallet.Balance
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "success",
			"data": UsersList,
		})
	})
	r.GET("/tx", func(c *gin.Context) {
		// List Transactions
		c.JSON(http.StatusOK, gin.H{
			"message": "success",
			"data": TransactionsList,
		})
	})
	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}

func worker (job Job) {
	fmt.Println(job, "================")
	if job.Action == "user" {
		verifyUser(job.Id)
	} else if job.Action == "tx" {
		completeTx(job.Id)
	} else {
		fmt.Printf("Wrong input!")
	}
}

func enQueue (j Job) {
	jobs <- j
	fmt.Println("Added job", j)
}


func verifyUser (id string) bool {
	fmt.Println(id, "=============== Verify user")
	userInfo := UsersList[id]
	if !userInfo.DontVerify {
		userInfo.IsVerified = true
		UsersList[id] = userInfo
	}
	return true
}

func completeTx (id string) bool {
	fmt.Println(id, "=============== Complete Tx")
	tx, ok := TransactionsList[id]
	if !ok {
		return false
	}
	senderId := tx.SenderId
	sender, ok := UsersList[senderId]
	if !ok {
		return false
	}
	receiverId := tx.ReceiverId
	receiver, ok := UsersList[receiverId]
	if !ok {
		return false
	}
	amount := tx.Amount
	if senderId == receiverId {
		message := "You cannot send funds to yourself"
		fmt.Println(message);
		tx.Status = "failed"
		tx.Reason = message
	}
	if !sender.IsVerified {
		message := "This user is not verified this transaction cannot be completed"
		fmt.Println(message)
		tx.Status = "failed"
		tx.Reason = message
	} else if !receiver.IsVerified {
		message := "The receiver of this fund is not verified, this transaction cannot be completed"
		fmt.Println(message)
		tx.Status = "failed"
		tx.Reason = message
	} else {
		sendersWallet := UsersWallet[senderId]
		receiversWallet := UsersWallet[receiverId]
		if sendersWallet.Balance < amount {
			message := "insufficient fund to complete this transaction."
			fmt.Println(message)
			tx.Status = "failed"
			tx.Reason = message
		} else {
			sendersWallet.Balance -= amount
			UsersWallet[senderId] = sendersWallet
			receiversWallet.Balance += amount
			UsersWallet[receiverId] = receiversWallet
			tx.Status = "completed"
		}
	}
	TransactionsList[id] = tx
	return true
}	