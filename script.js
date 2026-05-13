(function initParticles() {
  const canvas = document.getElementById('bg-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  let W, H, particles;

  const COLORS = ['#E07B3A', '#2B7BB9', '#3AAE6E', '#9B59B6'];
  const N = 80;

  function resize() {
    W = canvas.width  = window.innerWidth;
    H = canvas.height = window.innerHeight;
  }

  function makeParticle() {
    return {
      x:    Math.random() * W,
      y:    Math.random() * H,
      r:    Math.random() * 1.5 + 0.3,
      vx:   (Math.random() - 0.5) * 0.3,
      vy:   (Math.random() - 0.5) * 0.3,
      c:    COLORS[Math.floor(Math.random() * COLORS.length)],
      a:    Math.random() * 0.6 + 0.2,
    };
  }

  function init() {
    resize();
    particles = Array.from({ length: N }, makeParticle);
  }

  function draw() {
    ctx.clearRect(0, 0, W, H);

    for (let i = 0; i < particles.length; i++) {
      for (let j = i + 1; j < particles.length; j++) {
        const dx = particles[i].x - particles[j].x;
        const dy = particles[i].y - particles[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 140) {
          ctx.beginPath();
          ctx.strokeStyle = `rgba(255,255,255,${0.04 * (1 - dist / 140)})`;
          ctx.lineWidth = 0.5;
          ctx.moveTo(particles[i].x, particles[i].y);
          ctx.lineTo(particles[j].x, particles[j].y);
          ctx.stroke();
        }
      }
    }

    particles.forEach(p => {
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = p.c + Math.round(p.a * 255).toString(16).padStart(2, '0');
      ctx.fill();

      p.x += p.vx;
      p.y += p.vy;

      if (p.x < 0 || p.x > W) p.vx *= -1;
      if (p.y < 0 || p.y > H) p.vy *= -1;
    });

    requestAnimationFrame(draw);
  }

  window.addEventListener('resize', () => { resize(); });
  init();
  draw();
})();

(function initCounters() {
  const els = document.querySelectorAll('.stat-num[data-target]');
  if (!els.length) return;

  function animateCounter(el) {
    const target = parseInt(el.dataset.target, 10);
    const duration = 1800;
    const start = performance.now();

    function step(now) {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);

      const eased = 1 - Math.pow(1 - progress, 3);
      el.textContent = Math.round(eased * target).toLocaleString('es-PE');
      if (progress < 1) requestAnimationFrame(step);
    }

    requestAnimationFrame(step);
  }

  const observer = new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        animateCounter(e.target);
        observer.unobserve(e.target);
      }
    });
  }, { threshold: 0.5 });

  els.forEach(el => observer.observe(el));
})();

(function initScrollReveal() {
  const targets = document.querySelectorAll(
    '.arch-card, .kpi-card, .tech-item, .pipeline-step, .cmd-block, .event-chip'
  );

  const observer = new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        e.target.style.animationPlayState = 'running';
        observer.unobserve(e.target);
      }
    });
  }, { threshold: 0.12 });

  targets.forEach(el => {
    el.style.animationPlayState = 'paused';
    observer.observe(el);
  });
})();

(function initTerminal() {
  const streamLine = document.getElementById('stream-line');
  if (!streamLine) return;

  const eventTypes = [
    'venta_completada',
    'venta_iniciada',
    'stock_bajo',
    'venta_devuelta',
    'feedback_recibido',
    'descuento_aplicado',
  ];

  const canales = ['tienda_norte', 'tienda_sur', 'online'];
  const categorias = ['electronica', 'ropa', 'hogar', 'alimentos', 'deportes'];

  let counter = 1;

  function randomItem(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
  }

  function fakeEvent() {
    const id      = `EVT-${String(counter).padStart(7, '0')}`;
    const type    = randomItem(eventTypes);
    const canal   = randomItem(canales);
    const cat     = randomItem(categorias);
    const monto   = (Math.random() * 800 + 20).toFixed(2);
    const alerta  = (type === 'stock_bajo' && Math.random() > 0.6)
      ? ' ⚠ ALERTA_STOCK_BAJO'
      : (type === 'venta_devuelta' && parseFloat(monto) > 500)
        ? ' ⚠ ALERTA_DEVOLUCION_ALTO_VALOR'
        : '';

    counter++;
    return `Evento ${id} · ${type} · ${canal} · ${cat} · S/ ${monto}${alerta}`;
  }

  const body = document.querySelector('.terminal-body');
  let eventCount = 0;

  const terminalEl = document.querySelector('.terminal');
  const obs = new IntersectionObserver(entries => {
    if (entries[0].isIntersecting) {
      startTerminal();
      obs.unobserve(terminalEl);
    }
  }, { threshold: 0.3 });

  obs.observe(terminalEl);

  function startTerminal() {
    setInterval(() => {

      streamLine.textContent = fakeEvent();
      streamLine.style.color = streamLine.textContent.includes('⚠')
        ? '#E74C3C'
        : '#E07B3A';

      eventCount++;

      const newLine = document.createElement('div');
      newLine.className = 't-line out';
      newLine.style.fontSize = '0.74rem';
      newLine.style.opacity  = '0';
      newLine.style.transition = 'opacity 0.3s';
      newLine.textContent = streamLine.textContent;
      if (newLine.textContent.includes('⚠')) {
        newLine.style.color = '#E74C3C';
      }

      body.insertBefore(newLine, streamLine);
      requestAnimationFrame(() => { newLine.style.opacity = '1'; });

      const eventLines = body.querySelectorAll('.t-line.out:not(#stream-line)');
      if (eventLines.length > 10) {
        eventLines[0].style.opacity = '0';
        setTimeout(() => eventLines[0].remove(), 300);
      }

      streamLine.textContent = '▌';
      streamLine.style.color = '#E07B3A';
    }, 1200);
  }
})();

(function initNavActive() {
  const navbar = document.querySelector('.navbar');
  window.addEventListener('scroll', () => {
    if (window.scrollY > 60) {
      navbar.style.background = 'rgba(10,12,15,0.97)';
    } else {
      navbar.style.background = 'rgba(10,12,15,0.85)';
    }
  }, { passive: true });
})();