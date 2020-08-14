package jsondb

import (
    "fmt"
    "os"
    "io/ioutil"
    "errors"
    "strings"
    "reflect"
    "encoding/json"
)


type Options struct {
    /// Where to store the documetns
    Dir string
}

type Driver struct {
    Opt *Options
}

func New(opt *Options) (*Driver, error) {
    var err error
    
    
    err = os.MkdirAll(opt.Dir, os.ModeDir)
    if err != nil && os.IsNotExist(err) {
        return nil, err
    }
    
    fi, err := os.Stat(opt.Dir)
    if err != nil {
        return nil, err
    }
    
    if fi.IsDir() != true {
        return nil, errors.New("Storage path is not directory")
    }
    
    
    return &Driver{opt}, err
}


func (t *Driver) Write(col string, key string, i interface{}) error {
    os.MkdirAll(fmt.Sprintf("%s/%s", t.Opt.Dir, col), os.ModeDir)
    
    raw, err := json.Marshal(i)
    if err != nil {
        return err
    }
    
    err = ioutil.WriteFile(fmt.Sprintf("%s/%s/%s.json", t.Opt.Dir, col, key), raw, os.ModePerm)
    if err != nil {
        return err
    }
    
    return nil
}

func (t *Driver) Read(col string, key string, i interface{}) error {
    raw, err := ioutil.ReadFile(fmt.Sprintf("%s/%s/%s.json", t.Opt.Dir, col, key))
    if err != nil {
        return err
    }
    
    err = json.Unmarshal(raw, &i)
    if err != nil {
        return err
    }
    
    return nil
}

func (t *Driver) Delete(col string, key string) error {
    p := t.Path(col, key)
    _, err := os.Stat(p)
    if err != nil && os.IsNotExist(err) {
        return nil
    }
    
    err = os.Remove(p)
    return err
}

func (t *Driver) Keys(col string) ([]string, error) {
    dir := fmt.Sprintf("%s/%s", t.Opt.Dir, col)
    
    d, err := ioutil.ReadDir(dir)
    if err != nil {
        return nil, err
    }
    
    var keys []string
    for _, fi := range d {
        key := strings.TrimRight(fi.Name(), ".json")
        keys = append(keys, key)
    }
    
    return keys, nil
}

/// Get the absolute path of the document 
func (t *Driver) Path(col string, key string) string {
    return fmt.Sprintf("%s/%s/%s.json", t.Opt.Dir, col, key)
}

/// Get the storage dir path
func (t *Driver) DBDir() string {
    return t.Opt.Dir
}


func DeepEqualRaw(a, b []byte) bool {
    var aj, bj map[string]interface{}
    var err error
    
    err = json.Unmarshal(a, &aj)
    if err != nil {
        return false
    }
    err = json.Unmarshal(b, &bj)
    if err != nil {
        return false
    }
    
    return reflect.DeepEqual(aj, bj)
}
