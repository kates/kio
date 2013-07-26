var Kio = require("../lib/kio").Kio,
    kio = new Kio({name: "crawler", retries: 2});

for (var i = 0; i < 200; i++) {
  if (i % 2 === 0) {
    kio.defer("delayed-" + i, Math.ceil(Math.random() * 10000));
  } else {
    kio.enqueue("job-" + i);
  }
}

kio.close();
