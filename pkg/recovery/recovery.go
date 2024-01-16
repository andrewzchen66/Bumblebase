package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tl.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	el := &editLog{clientId, table.GetName(), action, key, oldval, newval}
	logs := rm.txStack[clientId]
	logs = append(logs, el)
	rm.txStack[clientId] = logs
	rm.writeToBuffer(el.toString())
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	sl := &startLog{clientId}
	rm.txStack[clientId] = []Log{sl}
	rm.writeToBuffer(sl.toString())

}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	delete(rm.txStack, clientId)
	cl := &commitLog{clientId}
	rm.writeToBuffer(cl.toString())
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	for _, table := range rm.d.GetTables() {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllUpdates()
	}
	ids := make([]uuid.UUID, 0, len(rm.txStack))
	for id := range rm.txStack {
		ids = append(ids, id)
	}
	cl := &checkpointLog{ids}
	for id := range rm.txStack {
		rm.txStack[id] = append(rm.txStack[id], cl)
	}
	rm.writeToBuffer(cl.toString())
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	// Get most recent checkpoint
	logs, checkpointPos, err := rm.readLogs()
	if err != nil {
		return err
	}
	// Keep track of currently running transactions
	txs := make(map[uuid.UUID]bool)
	cl, isCheckpoint := logs[checkpointPos].(*checkpointLog)
	if isCheckpoint {
		for _, id := range cl.ids {
			txs[id] = true
			err := rm.tm.Begin(id)
			if err != nil {
				return err
			}
		}
	}
	// Redo the logs after the checkpoint
	for i := checkpointPos; i < len(logs); i++ {
		log := logs[i]
		switch log := log.(type) {
		case *startLog:
			err := rm.tm.Begin(log.id)
			if err != nil {
				return err
			}
			txs[log.id] = true
		case *tableLog:
			err := rm.Redo(log)
			if err != nil {
				return err
			}
		case *editLog:
			err := rm.Redo(log)
			if err != nil {
				return err
			}
		case *commitLog:
			err := rm.tm.Commit(log.id)
			if err != nil {
				return err
			}
			delete(txs, log.id)
		case *checkpointLog:
			if i == checkpointPos {
				continue
			}
			return errors.New("checkpoint log not expected after checkpointPos")
		default:
			return errors.New("invalid log type")
		}
	}

	// Undo all transactions that have failed to commit, and commit them
	for i := len(logs) - 1; i >= 0; i-- {
		if len(txs) == 0 {
			break
		}
		switch log := logs[i].(type) {
		case *startLog:
			if _, exists := txs[log.id]; exists {
				err := rm.tm.Commit(log.id)
				if err != nil {
					return err
				}
				delete(txs, log.id)
			}
		case *editLog:
			if _, exists := txs[log.id]; exists {
				err := rm.Undo(log)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	// Obtain transaction from txStack, and check well-formedness
	logs := rm.txStack[clientId]
	if len(logs) == 0 {
		return errors.New("transaction has no logs")
	}
	switch logs[0].(type) {
	case *tableLog:
	case *startLog:
	default:
		return errors.New("transaction already commited")
	}
	// Iterate backwards to undo the transaction's actions
	for i := len(logs) - 1; i >= 0; i-- {
		if _, isEdit := logs[i].(*editLog); isEdit {
			err := rm.Undo(logs[i])
			if err != nil {
				return err
			}
		}
	}
	// Commit things so that we know rollback has ended
	rm.Commit(clientId)
	err := rm.tm.Commit(clientId)
	if err != nil {
		return err
	}
	return nil
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
