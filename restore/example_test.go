package restore_test

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/restore"
)

// ExampleBuildPlan shows producing a serializable, non-executing
// restore preview directly from an RDB file.
func ExampleBuildPlan() {
	f, err := os.Open("../cases/multiple_databases.rdb")
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	var objs []model.RedisObject
	dec := core.NewDecoder(f).WithSpecialOpCode()
	if err := dec.Parse(func(obj model.RedisObject) bool {
		objs = append(objs, obj)
		return true
	}); err != nil {
		panic(err)
	}

	plan, err := restore.BuildPlan(objs, restore.Options{
		ReferenceTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiredPolicy: restore.ExpiredPolicySkip,
		Target:        restore.ProfileRedis74,
	})
	if err != nil {
		panic(err)
	}

	out, _ := json.MarshalIndent(plan.Databases, "", "  ")
	fmt.Println(string(out))
	// Output:
	// [
	//   {
	//     "db": 0,
	//     "switchCommand": {
	//       "args": [
	//         "SELECT",
	//         "0"
	//       ]
	//     },
	//     "itemIds": [
	//       "db0/key/key_in_zeroth_database"
	//     ]
	//   },
	//   {
	//     "db": 2,
	//     "switchCommand": {
	//       "args": [
	//         "SELECT",
	//         "2"
	//       ]
	//     },
	//     "itemIds": [
	//       "db2/key/key_in_second_database"
	//     ]
	//   }
	// ]
}
