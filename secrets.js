const fs = require("fs")

module.exports = {
  // Get a secret from its name
  get (secret) {
    try {
      // Swarm secret are accessible within tmpfs /run/secrets dir
      return fs.readFileSync(`/run/secrets/${secret}`)
     }
     catch (e) {
       return false
     }
  }
}