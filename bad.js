
/* Admin tabs bootstrap (runs even if later script has a parse error) */
(function(){
  function qsa(sel){ return document.querySelectorAll(sel); }
  function setActive(tab){
    try{
      var btn = document.querySelector('.tabbtn[data-tab="'+tab+'"]');
      var minRole = (btn && btn.getAttribute && btn.getAttribute('data-minrole')) ? btn.getAttribute('data-minrole') : "manager";
      if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minRole] !== undefined){
        if(ROLE_RANK[ROLE] < ROLE_RANK[minRole]){
          try{ toast("Owner-only section"); }catch(e){}
          // snap back to Ops if a hash tried to open a locked tab
          try{
            if(tab !== "ops" && document.querySelector('.tabbtn[data-tab="ops"]')) tab = "ops";
          }catch(e){}
        }
      }
    }catch(e){}

    try{
      var btns = qsa('.tabbtn');
      for(var i=0;i<btns.length;i++){
        var b = btns[i];
        if(b && b.classList){
          var dt = b.getAttribute('data-tab');
          if(dt === tab) b.classList.add('active'); else b.classList.remove('active');
        }
      }
      var panes = qsa('.tabpane');
      for(var j=0;j<panes.length;j++){
        var p = panes[j];
        if(p && p.classList) p.classList.add('hidden');
      }
      var pane = document.getElementById('tab-'+tab);
      if(pane && pane.classList) pane.classList.remove('hidden');
      try{ history.replaceState(null,'','#'+tab); }catch(e){}
    }catch(e){}
  }
  window.showTab = function(tab){
  try{
    var b = document.querySelector('.tabbtn[data-tab="'+tab+'"]');
    var minr = (b && b.getAttribute) ? (b.getAttribute('data-minrole') || 'manager') : 'manager';
    if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minr] !== undefined){
      if(ROLE_RANK[ROLE] < ROLE_RANK[minr]){
        try{ toast('Owner only — redirected to Ops', 'warn'); }catch(e){}
        try{ setActive('ops'); }catch(e){}
        return false;
      }
    }
  }catch(e){}

  // switch tab panes
  try{ setActive(tab); }catch(e){}

  // Fan Zone: only init when the Fan Zone tab is selected
  if(tab === 'fanzone'){
    try{ initFanZoneAdmin(); }catch(e){}
  }

  return false;
};

  function bind(){
    var btns = qsa('.tabbtn');
    
    // mark owner-only tabs for managers
    try{
      for(var j=0;j<btns.length;j++){
        var br = btns[j];
        var minr = (br && br.getAttribute) ? (br.getAttribute('data-minrole')||'manager') : 'manager';
        if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minr] !== undefined){
          if(ROLE_RANK[ROLE] < ROLE_RANK[minr]){
            try{ br.classList.add('locked'); br.setAttribute('title','Owner only'); }catch(e){}
          }
        }
      }
    }catch(e){}
for(var i=0;i<btns.length;i++){
      (function(b){
        try{
          b.addEventListener('click', function(ev){
            try{ ev.preventDefault(); }catch(e){}
            var t = b.getAttribute('data-tab');
            if(t){
              try{
                var minr = (b && b.getAttribute) ? (b.getAttribute('data-minrole') || 'manager') : 'manager';
                if(ROLE_RANK && ROLE_RANK[ROLE] !== undefined && ROLE_RANK[minr] !== undefined){
                  if(ROLE_RANK[ROLE] < ROLE_RANK[minr]){ try{ toast('Owner only — redirected to Ops', 'warn'); }catch(e){} setActive('ops'); return; }
                }
              }catch(e){}
              setActive(t);
            }
          });
        }catch(e){}
      })(btns[i]);
    }
    // initial hash or default ops
    var h = (location.hash || '').replace('#','').trim();
    if(h && document.querySelector('.tabbtn[data-tab="'+h+'"]')) showTab(h);
    else showTab('ops');
    window.addEventListener('hashchange', function(){
      var t = (location.hash || '').replace('#','').trim();
      if(t && document.querySelector('.tabbtn[data-tab="'+t+'"]')) showTab(t);
    });
  }

  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', bind);
  else bind();
})();
