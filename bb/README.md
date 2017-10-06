Budget Bakers assessment task
=====
Run with `KEY=<aws-key> SECRET=<aws-secret> sbt`. Then execute sbt's `run` command to listen to requests with play framework.

There are three endpoints:

* GET **/status**: will get approximate number of messages. Navigate your browser to the url: `http://localhost:9000/status` or run from shell with: `curl http://localhost:9000/status`
* POST **/push**: will put 10k messages to the queue, runs approximately 10s on my machine. Runs approximately 15s on my machine. Run this route from shell with: `curl -X POST http://localhost:9000/push`
* POST **/receive**: will read 10k messages from the queue, runs approximately 10s on my machine. The end file has ~700k of data in it. Run this route from shell with: `curl -X POST http://localhost:9000/receive`

The implementation is in `controllers.HomeController` class. The rest of files are just from the play framework template.
