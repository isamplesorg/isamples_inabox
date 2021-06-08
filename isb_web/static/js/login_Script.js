
var link = document.getElementById('Git')


const url = `${app_url}/login/Git`

fetch(url)
    .then(res => res.json())
    .then(body => {
        link.href = body.url
    })