// World Cup Concierge Landing — v4
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
  
  // ---------- A/B hero headline (safe) ----------
  const h1 = qs(".heroH1");
  const AB_KEY = "ab_hero_v1";
  const ab = localStorage.getItem(AB_KEY) || (Math.random()<0.5?"A":"B");
  localStorage.setItem(AB_KEY, ab);
  if(h1){
    h1.textContent = ab==="A"
      ? "Match-night chaos → calm control."
      : "Turn packed nights into profit—automatically.";
  }
  if(window.gtag){ gtag("event","ab_view",{variant:ab}); }

  // Funnel events
  const demoBtn = qs("#demoBtnTop");
  demoBtn && demoBtn.addEventListener("click", ()=>{
    window.gtag && gtag("event","demo_click",{variant:ab});
  });

  let formStarted=false;
  form && form.addEventListener("focusin", ()=>{
    if(formStarted) return;
    formStarted=true;
    window.gtag && gtag("event","form_start",{variant:ab});
  });

})();

  // ---------- Hero video + sound toggle ----------
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

  // ---------- Carousel controls ----------
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

  // ---------- A/B hero headline (safe) ----------
  const h1 = qs(".heroH1");
  const AB_KEY = "ab_hero_v1";
  const ab = localStorage.getItem(AB_KEY) || (Math.random()<0.5?"A":"B");
  localStorage.setItem(AB_KEY, ab);
  if(h1){
    h1.textContent = ab==="A"
      ? "Match-night chaos → calm control."
      : "Turn packed nights into profit—automatically.";
  }
  if(window.gtag){ gtag("event","ab_view",{variant:ab}); }

  // Funnel events
  const demoBtn = qs("#demoBtnTop");
  demoBtn && demoBtn.addEventListener("click", ()=>{
    window.gtag && gtag("event","demo_click",{variant:ab});
  });

  let formStarted=false;
  form && form.addEventListener("focusin", ()=>{
    if(formStarted) return;
    formStarted=true;
    window.gtag && gtag("event","form_start",{variant:ab});
  });

})();