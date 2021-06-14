
var link = document.getElementById('Git')


const url =  `http://0.0.0.0:8000/login/Git`

fetch(url)
    .then(res => res.json())
    .then(body => {
        link.href = body.url
    })