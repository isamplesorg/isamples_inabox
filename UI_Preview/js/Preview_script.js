//create split bar for table and content
Split(["#records_table", "#info"], {
    direction: "vertical",
    sizes: [30, 70],
    gutterSize: 30,
});

var infoSplit = Split(["#InfoLeft", "#InfoRight"], {
    gutterSize: 10,
    minSize: [200, 200],
})

//filter the records accordig users' input
function doIdFilter(eid) {
    var e = document.getElementById(eid).value;
    console.log("Filter ID: " + e);
    table.setFilter("id", "like", e);
}

//enable input submit when users press enter
var inputText = document.getElementById("id_filter");
inputText.addEventListener("keyup", function(event) {
    if (event.keyCode === 13) {
        event.preventDefault();
        document.getElementById("bt_filter").click();
    }
})

//load records types
var tableType = new Tabulator("#types_table", {
    layout: "fitColumns",
    placeholder: "No data availble",
    ajaxURL: "https://mars.cyverse.org/thing/types",
    height: "300px",
    columns: [{
        "title": "Item Types",
        field: "item_type",
    }, {
        "title": "Count",
        field: "count",
    }]
})

//change the raw data and iSample data panels directions
function changePanes(direction) {
    var container = document.getElementsByClassName("infoContainer")[0];
    if (direction == 'h') {
        container.style.flexDirection = "row";
        infoSplit.destroy();
        infoSplit = Split(["#InfoLeft", "#InfoRight"], {
            gutterSize: 10,
            minSize: [200, 200],
        })
    } else {
        container.style.flexDirection = "column";
        infoSplit.destroy();
        infoSplit = Split(["#InfoLeft", "#InfoRight"], {
            gutterSize: 10,
            minSize: [100, 100],
            direction: "vertical",
        })
    }
}


//table ---------------------------------------------------------------------
//show specified identifier record
async function showRawRecord(id) {
    var url = `https://mars.cyverse.org/thing/${id}?full=false`;
    console.log(url);
    fetch(url)
        .then(response => response.json())
        .then(doc => {
            var e = document.getElementById("orginal_show");
            e.innerHTML = prettyPrintJson.toHtml(doc, FormatOptions = {
                indent: 2
            });
        })
}

//load records types
var tableType = new Tabulator("#types_table", {
    layout: "fitColumns",
    placeholder: "No data availble",
    ajaxURL: "https://mars.cyverse.org/thing/types",
    height: "300px",
    columns: [{
        "title": "Item Types",
        field: "item_type",
    }, {
        "title": "Count",
        field: "count",
    }]
})



//select row and show record informaton
function rowClick(e, row) {
    console.log(row._row);
    if (row._row.element.value == undefined) {
        var id = row._row.data.id;
        var reportId = document.getElementById('currentID');
        reportId.value = id;
        reportId.innerHTML = id;
        var pres = document.getElementsByTagName("pre");
        for (var i = 0; i < pres.length; i++) {
            pres[i].style.overflow = "auto";
        }
        showRawRecord(id);
    } else {
        document.getElementsByClassName("hasBug")[0].style.display = "block";
        document.getElementById("orginal_show").innerHTML = "";
        document.getElementById("core_show").innerHTML = "";
    }

}

document.getElementById("apply_bug").addEventListener("click", function() {
    document.getElementsByClassName("hasBug")[0].style.display = "none";
})

//update load data
function Dataloaded(url, params, response) {
    var e1 = document.getElementById("records_loaded");
    var e2 = document.getElementById("total_records");
    console.log(response);
    e1.innerHTML = parseInt(e1.innerHTML) + response.data.length;
    e2.innerHTML = response.total_records;
    return response;
}

//load records from server into tabulator
var table = new Tabulator("#records_table", {
    layout: "fitColumns",
    placeholder: "No data availble",
    ajaxURL: "https://mars.cyverse.org/thing/",
    paginationDataSent: {
        "page": "offset",
        "size": "limit",
    },

    ajaxRequesting: function(url, params) {
        params.offset = "" + (parseInt(params.offset) - 1) * parseInt(params.limit);
    },
    ajaxProgressiveLoad: "scroll",
    paginationSize: 1000,
    columns: [{
        "title": "ID",
        field: "id",
        width: 150
    }, {
        "title": "Authority ID",
        field: "authority_id",
        width: 150
    }, {
        "title": "Time Created",
        field: "tcreated",
        width: 300
    }, {
        "title": "Status",
        field: "resolved_status",
        width: 100
    }, {
        "title": "URL",
        field: "resolved_url",
        width: 600
    }, {
        "title": "Elapsed",
        field: "resolve_elapsed"
    }],
    ajaxResponse: Dataloaded,
    rowClick: rowClick,
    selectable: 1
});

//add types in a popup box
var modal = document.getElementsByClassName("types")[0];
var bt_types = document.getElementById("bt_types");
var closeBt = document.getElementsByClassName("closeBt")[0];

bt_types.addEventListener('click', function() { modal.style.display = "block"; });
closeBt.addEventListener('click', function() { modal.style.display = "none"; });




//report popup-----------------------------------------------------------------
var reportModel = document.getElementsByClassName("report")[0];
var bt_report = document.getElementsByClassName("bt_report")[0];
var repClose = document.getElementById("repClose");
var repText = document.getElementsByClassName("retext")[0];

bt_report.addEventListener("click", function() {
    reportModel.style.display = "block";
    repText.value = "";
    repText.placeholder = "Please enter bug";
});
repClose.addEventListener("click", function() {
    reportModel.style.display = "none";
})

var reportTitle = document.getElementById('currentID');
var reportBody = document.getElementById('reportBody');
var bt_issue = document.getElementById('bt_issue');
bt_issue.addEventListener('click', createIssue);

async function createIssue() {
    if (reportTitle.value == undefined) {
        alert("Please choose a record!");
    } else {
        //const url = `http://localhost:2400/issues?title=${reportTitle.value}&report=${reportBody.value}`
        //fetch(url)

        reportModel.style.display = "none";
    }
}


window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }

    if (event.target == reportModel) {
        reportModel.style.display = "none";
    }
}

//field selection
var fieldContainer = document.getElementById("fields");
var bt_field = document.getElementById("bt_field");

bt_field.addEventListener("click", popupField)

function popupField() {
    if (bt_field.value == "") {
        bt_field.value = "active"
        fieldContainer.style.display = "block";
        fieldContainer.style.top = (bt_field.offsetTop + bt_field.offsetHeight + 5) + "px";
        fieldContainer.style.left = (bt_field.offsetLeft - fieldContainer.offsetWidth + bt_field.offsetWidth / 2) + "px";
    } else {
        bt_field.value = ""
        fieldContainer.style.display = "none";
    }
}

var bt_apply = document.getElementById("apply_field");

bt_apply.addEventListener("click", setColumns);

var newCol = [];

function setColumns() {
    console.log("true")
    newCol = [];
    if (document.getElementById('id').checked) {
        newCol.push({
            "title": "Id",
            field: "id",
            width: 150
        });
    }
    if (document.getElementById('Authority Id').checked) {
        newCol.push({
            "title": "Authority Id",
            field: "authority_id"
        });
    }
    if (document.getElementById('Time Created').checked) {
        newCol.push({
            "title": "Time Created",
            field: "tcreated",
            width: 300
        });
    }
    if (document.getElementById('status').checked) {
        newCol.push({
            "title": "status",
            field: "resolved_status"
        });
    }
    if (document.getElementById('url').checked) {
        newCol.push({
            "title": "url",
            field: "resolved_url",
            width: 600
        });
    }
    if (document.getElementById('elapsed').checked) {
        newCol.push({
            "title": "elapsed",
            field: "resolve_elapsed"
        });
    }
    table.setColumns(newCol);
}

//report popup-----------------------------------------------------------------
var reportModel = document.getElementsByClassName("report")[0];
var bt_report = document.getElementsByClassName("bt_report")[0];
var repClose = document.getElementById("repClose");
var repText = document.getElementsByClassName("retext")[0];

bt_report.addEventListener("click", function() {
    reportModel.style.display = "block";
    document.getElementsByClassName("reportModel")[0].style.display = "block";
    repText.value = "";
    repText.placeholder = "Please enter issues";
});
repClose.addEventListener("click", function() {
    reportModel.style.display = "none";
})

var reportTitle = document.getElementById('currentID');
var reportBody = document.getElementById('reportBody');
var bt_issue = document.getElementById('bt_issue');
bt_issue.addEventListener('click', createIssue);
async function createIssue() {
    if (reportTitle.value == undefined) {
        alert("Please choose a record!");
    } else {
        document.getElementsByClassName("reportModel")[0].style.display = "none";
        document.getElementsByClassName("feedback")[0].style.display = "block";
    }
}
document.getElementById("bt_feedback").addEventListener("click", function() {
    table.getSelectedRows()[0]._row.element.style.backgroundColor = "yellow";
    table.getSelectedRows()[0]._row.element.value = "HasBug";
    document.getElementsByClassName("feedback")[0].style.display = "none";
    reportModel.style.display = "none";
})


//log out
var logout = document.getElementById("bt_logout");
logout.addEventListener('click', function() {
    location.href = "./login.html";
})

//filter pop box
var filterContainer = document.getElementById("filter");
var bt_filter = document.getElementById("bt_sorter");

bt_filter.addEventListener("click", popupfilter)

function popupfilter() {
    if (bt_filter.value == "") {
        bt_filter.value = "active"
        filterContainer.style.display = "block";
        filterContainer.style.top = (bt_filter.offsetTop + bt_filter.offsetHeight + 5) + "px";
        filterContainer.style.left = (bt_filter.offsetLeft - filterContainer.offsetWidth + bt_filter.offsetWidth / 2) + "px";
    } else {
        bt_filter.value = ""
        filterContainer.style.display = "none";
    }
}