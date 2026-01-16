// landing/app.js
(function(){
  // ---- Config ----
  // Choose your demo flow:
  //  - "form": posts to /api/lead (recommended)
  //  - "calendly": opens your Calendly link
  const DEMO_MODE = "form";
  const CALENDLY_URL = "https://calendly.com/YOUR-LINK"; // <- optional

  // ---- Helpers ----
  const qs = (sel, el=document) => el.querySelector(sel);
  const qsa = (sel, el=document) => Array.from(el.querySelectorAll(sel));
  const nowIso = () => new Date().toISOString();

  function getUtm(){
    const u = new URL(location.href);
    const keys = ["utm_source","utm_medium","utm_campaign","utm_term","utm_content"];
    const out = {};
    keys.forEach(k=>{ const v=u.searchParams.get(k); if(v) out[k]=v; });
    return out;
  }

  function getSessionId(){
    try{
      const k="wcc_landing_sid";
      let v = localStorage.getItem(k);
      if(!v){
        v = (crypto.randomUUID ? crypto.randomUUID() : (Math.random().toString(16).slice(2)+Date.now().toString(16)));
        localStorage.setItem(k,v);
      }
      return v;
    }catch(e){ return "anon"; }
  }

  const SID = getSessionId();
  const UTM = getUtm();

  function track(event, details){
    const payload = {
      event: String(event||""),
      ts: nowIso(),
      sid: SID,
      url: location.href,
      ref: document.referrer || "",
      ...UTM,
      details: details || {}
    };
    // Prefer sendBeacon
    try{
      const blob = new Blob([JSON.stringify(payload)], {type:"application/json"});
      if(navigator.sendBeacon && navigator.sendBeacon("/api/track", blob)) return;
    }catch(e){}
    // Fallback fetch (keepalive helps on unload)
    try{
      fetch("/api/track", {
        method:"POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload),
        keepalive: true
      }).catch(()=>{});
    }catch(e){}
  }

  // ---- Scroll-based reveal animations ----
  const revealEls = qsa("[data-reveal]");
  if("IntersectionObserver" in window){
    const io = new IntersectionObserver((entries)=>{
      entries.forEach(en=>{
        if(en.isIntersecting){
          en.target.classList.add("in");
          io.unobserve(en.target);
        }
      });
    }, {threshold: 0.14});
    revealEls.forEach(el=>io.observe(el));
  }else{
    revealEls.forEach(el=>el.classList.add("in"));
  }

  // ---- Scroll depth analytics ----
  let maxPct = 0;
  const marks = [25,50,75,100];
  const sent = new Set();
  function onScroll(){
    const doc = document.documentElement;
    const scrollTop = doc.scrollTop || document.body.scrollTop || 0;
    const height = (doc.scrollHeight - doc.clientHeight) || 1;
    const pct = Math.min(100, Math.round((scrollTop/height)*100));
    if(pct > maxPct) maxPct = pct;
    marks.forEach(m=>{
      if(!sent.has(m) && pct >= m){
        sent.add(m);
        track("scroll_depth", {pct:m});
      }
    });
  }
  window.addEventListener("scroll", onScroll, {passive:true});

  // ---- Hero video analytics ----
  const v = qs("#heroVideo");
  if(v){
    let played=false;
    v.addEventListener("play", ()=>{
      if(!played){ played=true; track("video_play"); }
    });
    // watch timers (only while playing)
    let t=0, int=null;
    function startTimer(){
      if(int) return;
      int=setInterval(()=>{
        if(!v.paused && !v.ended){
          t++;
          if(t===5) track("video_watch", {seconds:5});
          if(t===15) track("video_watch", {seconds:15});
          if(t===30) track("video_watch", {seconds:30});
        }
      }, 1000);
    }
    function stopTimer(){ if(int){ clearInterval(int); int=null; } }
    v.addEventListener("play", startTimer);
    v.addEventListener("pause", stopTimer);
    v.addEventListener("ended", stopTimer);
  }

  // Sound toggle analytics (button exists in hero)
  const soundBtn = qs("#soundToggle");
  if(soundBtn && v){
    soundBtn.addEventListener("click", ()=>{
      // sound toggle is handled in inline script too; we just track state
      setTimeout(()=>track("video_sound_toggle", {muted: v.muted}), 0);
    });
  }

  // ---- Demo button wiring ----
  const demoBtns = qsa('a[href="#demo"], a[href="#demo"] *').map(x=>x.closest('a[href="#demo"]')).filter(Boolean);
  demoBtns.forEach(btn=>{
    btn.addEventListener("click", ()=>track("click_demo"));
  });

  // If Calendly mode, override demo CTA to open link
  if(DEMO_MODE === "calendly"){
    demoBtns.forEach(btn=>{
      btn.addEventListener("click", (e)=>{
        e.preventDefault();
        track("open_calendly");
        window.open(CALENDLY_URL, "_blank", "noopener");
      }, {capture:true});
    });
  }

  // ---- Lead form submit ----
  const form = qs("#leadForm");
  const status = qs("#leadStatus");
  function setStatus(msg, ok){
    if(!status) return;
    status.textContent = msg || "";
    status.style.opacity = msg ? "1" : "0";
    status.dataset.ok = ok ? "1" : "0";
  }

  if(form){
    form.addEventListener("submit", async (e)=>{
      e.preventDefault();
      setStatus("Submitting…", true);
      track("lead_submit_attempt");

      const data = Object.fromEntries(new FormData(form).entries());
      const payload = {
        ...data,
        ts: nowIso(),
        sid: SID,
        page: location.href,
        ref: document.referrer || "",
        utm: UTM,
        source: "landing"
      };

      try{
        const res = await fetch("/api/lead", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify(payload)
        });
        const j = await res.json().catch(()=>({}));
        if(res.ok && (j.ok !== False)){
          setStatus("✅ Request received. We'll reach out shortly.", true);
          track("lead_submit_success");
          form.reset();
        }else{
          setStatus("⚠️ Could not submit. Try again or email hello@worldcupconcierge.app", false);
          track("lead_submit_fail", {status: res.status, resp: j});
        }
      }catch(err){
        setStatus("⚠️ Network error. Try again or email hello@worldcupconcierge.app", false);
        track("lead_submit_fail", {error: String(err)});
      }
    });
  }

  // ---- Page view ----
  track("page_view");
})();