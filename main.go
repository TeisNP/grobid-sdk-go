package grobid

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/imroc/req"
)

// Service is string of available services
type Service string

// all available services
const (
	FullTextDocument Service = "processFulltextDocument"
)

var (
	baseURL    string
	numWorkers = 10

	jobs chan file
	wg   = *new(sync.WaitGroup)
)

func init() {
	grobidHost, found := os.LookupEnv("GROBID_HOST")
	if !found {
		grobidHost = "localhost"
	}

	grobidPort, found := os.LookupEnv("GROBID_PORT")
	if !found {
		grobidPort = "8070"
	}

	baseURL = "http://" + grobidHost + ":" + grobidPort + "/api/"
}

func initConnection() error {
	resp, err := http.Get(baseURL + "isalive")
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("GROBID server does not appear up and running " + resp.Status)
	}

	fmt.Println("GROBID server is up and running")
	return nil
}

func Process(inputDir string, outputDir string, service Service) error {
	if err := initConnection(); err != nil {
		return err
	}

	jobs = make(chan file)

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, outputDir, service)
	}

	err := filepath.Walk(inputDir, visit)
	if err != nil {
		return err
	}
	close(jobs)

	wg.Wait()

	return nil
}

type file struct {
	Path string
	Name string
}

func visit(path string, f os.FileInfo, err error) error {
	if f.IsDir() {
		return nil
	}

	if strings.HasSuffix(string(f.Name()), ".pdf") || strings.HasSuffix(string(f.Name()), ".PDF") {
		jobs <- file{Path: path, Name: string(f.Name())}
	}

	return nil
}

func worker(id int, outputDir string, service Service) {
	defer wg.Done()

	currentServiceURL := baseURL + string(service)

	for fileJob := range jobs {
		fmt.Println("worker", id, "started job on file", fileJob.Path)
		if err := processPdf(fileJob, outputDir, currentServiceURL); err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("worker", id, "finished job on file", fileJob.Path)
	}
}

func processPdf(fileJob file, outputDir string, currentServiceURL string) error {
	outFilePath := outputDir + fileJob.Name + ".tei.xml"
	if _, err := os.Stat(outFilePath); err == nil {
		return errors.New("File alread exists " + outFilePath)
	}

	file, err := os.Open(fileJob.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	resp, err := req.Post(currentServiceURL, req.FileUpload{
		File:      file,
		FieldName: "input",
		FileName:  fileJob.Name,
	})
	if err != nil {
		return err
	}

	fout, err := os.Create(outFilePath)
	if err != nil {
		return err
	}
	defer fout.Close()

	fout.Write(resp.Bytes())

	return nil
}
