/*
 * ftp-s3 (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fclairamb/ftpserver/server"
	"github.com/harshavardhana/ftp-s3/s3"
)

func main() {
	confFile := flag.String("conf", "settings.ini", "Configuration file")
	dataDir := flag.String("data", "", "Data directory")

	flag.Parse()

	driver, err := s3.NewDriver(*confFile, *dataDir, os.Getenv("ENDPOINT"), os.Getenv("ACCESS_KEY"), os.Getenv("SECRET_KEY"))
	if err != nil {
		log.Fatalln(err)
	}

	ftpServer := server.NewFtpServer(driver)
	go func(ftpServer *server.FtpServer) {
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGTERM)
		for {
			switch <-ch {
			case syscall.SIGTERM:
				ftpServer.Stop()
				break
			}
		}
	}(ftpServer)
	log.Fatalln(ftpServer.ListenAndServe())
}
