// World Cup Concierge Landing — Premium (locked)
(function(){
  const qs = (sel, el=document) => el.querySelector(sel);
  const qsa = (sel, el=document) => Array.from(el.querySelectorAll(sel));
  const nowIso = () => new Date().toISOString();

  // ---------- Reveal animations ----------
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

  // ---------- Footer year ----------
  const yr = qs("#yr");
  if(yr) yr.textContent = String(new Date().getFullYear());

  // ---------- Countdown (June 11, 2026 UTC) ----------
  (function(){
    const target = new Date("2026-06-11T00:00:00Z").getTime();
    const pad = n => String(n).padStart(2,"0");
    function tick(){
      let t = Math.max(0, target - Date.now());
      const d = Math.floor(t/86400000); t%=86400000;
      const h = Math.floor(t/3600000);  t%=3600000;
      const m = Math.floor(t/60000);    t%=60000;
      const s = Math.floor(t/1000);
      const dEl = qs("#d"), hEl = qs("#h"), mEl = qs("#m"), sEl = qs("#s");
      if(dEl) dEl.textContent = d;
      if(hEl) hEl.textContent = pad(h);
      if(mEl) mEl.textContent = pad(m);
      if(sEl) sEl.textContent = pad(s);
    }
    tick(); setInterval(tick, 1000);
  })();

  // ---------- Force loop BOTH hero videos + cut before credits ----------
  (function(){
    const fg = qs("#heroVideo");                 // foreground
    const bg = qs(".heroVideo--blur");           // background

    const START_AT = 0.0;
    const TRIM_SECONDS = 5.5;  // locked per request
    const MIN_DUR = 6;

    function forceLoop(v, {forceMuted}){
      if(!v) return;

      if(forceMuted) v.muted = true; // DO NOT force mute foreground (sound toggle controls it)
      v.playsInline = true;

      let dur = 0;
      const safePlay = () => v.play().catch(()=>{});

      v.addEventListener("loadedmetadata", ()=>{
        dur = Number(v.duration || 0);
        safePlay();
      });

      v.addEventListener("timeupdate", ()=>{
        if(!dur || !isFinite(dur) || dur < MIN_DUR) return;
        if(v.currentTime >= (dur - TRIM_SECONDS)){
          try{ v.currentTime = START_AT; }catch(e){}
          safePlay();
        }
      });

      v.addEventListener("ended", ()=>{
        try{ v.currentTime = START_AT; }catch(e){}
        safePlay();
      });

      safePlay();
    }

    // Foreground: start muted to allow autoplay; user can unmute via toggle
    if(fg) fg.muted = true;

    forceLoop(fg, {forceMuted:false});
    forceLoop(bg, {forceMuted:true});
  })();

  // ---------- Hero sound toggle (foreground only) ----------
  const v = qs("#heroVideo");
  const toggle = qs("#soundToggle");
  function setIcon(){
    if(!toggle || !v) return;
    toggle.textContent = v.muted ? "🔇" : "🔊";
  }
  if(v){
    v.play().catch(()=>{});
  }
  if(toggle && v){
    setIcon();
    toggle.addEventListener("click", ()=>{
      v.muted = !v.muted;
      setIcon();
      if(v.paused) v.play().catch(()=>{});
    });
  }

  // ---------- Rolex tick + gold micro-glint on number update ----------
  (function(){
    const ids = ["d","h","m","s"];
    const last = { d:null, h:null, m:null, s:null };

    function pulse(el){
      if(!el) return;
      el.classList.remove("rolexTick","goldGlint");
      void el.offsetWidth; // reflow
      el.classList.add("rolexTick","goldGlint");
    }

    setInterval(()=>{
      ids.forEach(id=>{
        const el = document.getElementById(id);
        if(!el) return;
        const val = el.textContent;
        if(last[id] === null){ last[id] = val; return; }
        if(val !== last[id]){
          last[id] = val;
          pulse(el);
        }
      });
    }, 250);
  })();

  // ---------- Carousel controls (current behavior; we’ll fix rotation after you paste carousel snippet) ----------
  const track = qs("#carouselTrack");
  const prev = qs("#prevSlide");
  const next = qs("#nextSlide");
  function scrollBySlide(dir){
    if(!track) return;
    const slide = track.querySelector(".slide");
    if(!slide) return;
    const w = slide.getBoundingClientRect().width + 14; // include gap
    track.scrollBy({left: dir * w, behavior: "smooth"});
  }
  if(prev) prev.addEventListener("click", ()=>scrollBySlide(-1));
  if(next) next.addEventListener("click", ()=>scrollBySlide(1));

  // Optional gentle auto-advance (stops on user interaction)
  let auto = true;
  const stopAuto = ()=>{ auto=false; };
  ["wheel","touchstart","pointerdown","keydown"].forEach(ev=>{
    window.addEventListener(ev, stopAuto, {passive:true, once:true});
  });
  setInterval(()=>{
    if(!auto || !track) return;
    scrollBySlide(1);
  }, 4200);

  // ---------- Lead form wiring (POST /api/lead) ----------
  const form = qs("#leadForm");
  const status = qs("#leadStatus");
  function setStatus(msg, ok){
    if(!status) return;
    status.textContent = msg || "";
    status.dataset.ok = ok ? "1" : "0";
  }
  if(form){
    form.addEventListener("submit", async (e)=>{
      e.preventDefault();
      setStatus("Submitting…", true);

      const data = Object.fromEntries(new FormData(form).entries());
      const payload = { ...data, ts: nowIso(), source: "landing" };

      try{
        const res = await fetch("/api/lead", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(payload)
        });
        const j = await res.json().catch(()=>({}));
        if(res.ok && (j.ok !== false)){
          setStatus("✅ Request received. We'll reach out shortly.", true);
          form.reset();
        }else{
          setStatus("⚠️ Could not submit. Email hello@worldcupconcierge.app", false);
        }
      }catch(err){
        setStatus("⚠️ Network error. Email hello@worldcupconcierge.app", false);
      }
    });
  }

  // ---------- Scroll cue hide on first scroll ----------
  (function(){
    const cue = document.getElementById("scrollCue");
    if(!cue) return;
    const hide = ()=> cue.classList.add("is-hidden");
    const show = ()=> cue.classList.remove("is-hidden");

    function onScroll(){
      if(window.scrollY > 20){
        hide();
        window.removeEventListener("scroll", onScroll, {passive:true});
      }
    }
    window.addEventListener("scroll", onScroll, {passive:true});
    if(window.scrollY > 20) hide(); else show();
  })();

})();