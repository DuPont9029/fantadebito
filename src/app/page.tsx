"use client";
import { useEffect, useState } from "react";
// Autenticazione ora è server-side: usiamo /api/login

type Session = { userId: string; username: string; is_admin?: boolean } | null;
type Profile = { wins: number; losses: number } | null;

type SospensioneItem = { subject: string; grade: number };
type StanceItem = { userId: string; stance?: "favorevole" | "contrario" };
type ApiBet = Partial<SharedBet> & {
  sospensione?: Array<Record<string, unknown>>;
  stances?: Array<Record<string, unknown>>;
  participants?: Array<unknown>;
};
type SharedBet = {
  id: string;
  owner_id: string;
  subject: string;
  esito: "ammissione" | "sospensione" | "non_ammissione";
  sospensione: SospensioneItem[];
  invite_code: string;
  participants: string[];
  stances?: StanceItem[];
  created_at: string;
  terminated_at?: string;
  realized?: string;
};

function useLocalSession() {
  const [session, setSession] = useState<Session>(null);
  useEffect(() => {
    const raw = localStorage.getItem("fantadebito_session");
    if (raw) setSession(JSON.parse(raw));
  }, []);
  const save = (s: Session) => {
    setSession(s);
    if (s) localStorage.setItem("fantadebito_session", JSON.stringify(s));
    else localStorage.removeItem("fantadebito_session");
  };
  return { session, save };
}

// Scommesse locali rimosse: ora esistono solo scommesse condivise via API

export default function Home() {
  const { session, save } = useLocalSession();
  const [profile, setProfile] = useState<Profile>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string>("");
  const [isRegister, setIsRegister] = useState(false);
  // Editing account
  const [isEditingAccount, setIsEditingAccount] = useState<boolean>(false);
  const [editUsername, setEditUsername] = useState<string>("");
  const [editPassword, setEditPassword] = useState<string>("");
  const [accountError, setAccountError] = useState<string>("");
  const [accountInfo, setAccountInfo] = useState<string>("");
  // Stato per scommesse condivise
  const [sharedBets, setSharedBets] = useState<SharedBet[]>([]);
  const [sharedError, setSharedError] = useState<string>("");
  const [sharedInfo, setSharedInfo] = useState<string>("");
  // Campi Nome/Cognome e derivazione soggetto: prima lettera del nome + prime tre lettere del cognome
  const [createNome, setCreateNome] = useState<string>("");
  const [createCognome, setCreateCognome] = useState<string>("");
  function deriveSubjectFromNameSurname(nome: string, cognome: string): string {
    const n = (nome || "").trim();
    const c = (cognome || "").trim();
    if (!n && !c) return "";
    const first = n.slice(0, 1);
    const last3 = c.slice(0, 3);
    if (first && last3) return `${first}${last3}`;
    const single = (n || c);
    return `${single.slice(0, 1)}${single.slice(1, 4)}`;
  }
  const derivedSubject = deriveSubjectFromNameSurname(createNome, createCognome);
  const [createEsito, setCreateEsito] = useState<string>("ammissione");
  const [createStance, setCreateStance] = useState<string>("favorevole");
  const [sospensioneItems, setSospensioneItems] = useState<SospensioneItem[]>([{ subject: "", grade: 0 }]);
  const [joinStances, setJoinStances] = useState<Record<string, "favorevole" | "contrario">>({});
  const [loadingShared, setLoadingShared] = useState<boolean>(false);

  // Inizializza storage S3 creando users.parquet se manca
  useEffect(() => {
    (async () => {
      try {
        await fetch("/api/init", { method: "POST" });
      } catch (e) {
        console.warn("Init S3 failed", e);
      }
    })();
  }, []);

  // Rimosso: opzioni per scommesse locali

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    try {
      if (!isRegister) {
        const res = await fetch("/api/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password }),
        });
        const data = await res.json();
        if (!res.ok) {
          setError(data?.message || "Credenziali non valide");
          return;
        }
        const user = data.user as { id: string; username: string; is_admin?: boolean };
        save({ userId: user.id, username: user.username, is_admin: Boolean(user.is_admin) });
        await refreshProfile(user.id);
      } else {
        const res = await fetch("/api/register", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password }),
        });
        const data = await res.json();
        if (!res.ok) {
          setError(data?.message || "Registrazione fallita");
          return;
        }
        save({ userId: data.user.id, username: data.user.username, is_admin: false });
        await refreshProfile(data.user.id);
      }
    } catch (e) {
      const err = e as Error;
      setError(err?.message || "Errore di autenticazione");
    }
  }

  async function refreshProfile(uid?: string) {
    const userId = uid || session?.userId;
    if (!userId) return;
    try {
      const res = await fetch("/api/profile", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId }),
      });
      const data = await res.json();
      if (res.ok) {
        setProfile({ wins: Number(data.user?.wins ?? 0), losses: Number(data.user?.losses ?? 0) });
      }
    } catch (_) {}
  }

  async function refreshSharedBets() {
    if (!session?.userId) return;
    setLoadingShared(true);
    setSharedError("");
    try {
      const res = await fetch("/api/bets/list", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      const data = await res.json();
      if (!res.ok) {
        setSharedError(data?.message || "Caricamento scommesse fallito");
      } else {
        const bets: SharedBet[] = Array.isArray(data.bets)
          ? data.bets.map((b: unknown) => {
              const ab = b as ApiBet;
              const sosp: SospensioneItem[] = Array.isArray(ab.sospensione)
                ? ab.sospensione.map((s) => {
                    const rs = s as Record<string, unknown>;
                    return { subject: String(rs.subject ?? ""), grade: Number(rs.grade ?? 0) };
                  })
                : [];
              const parts: string[] = Array.isArray(ab.participants) ? ab.participants.map((p) => String(p)) : [];
              const stances: StanceItem[] = Array.isArray(ab.stances)
                ? ab.stances.map((s) => {
                    const rs = s as Record<string, unknown>;
                    const st = rs.stance === "contrario" ? "contrario" : rs.stance === "favorevole" ? "favorevole" : undefined;
                    return { userId: String(rs.userId ?? ""), stance: st };
                  })
                : [];
              return {
                id: String(ab.id ?? ""),
                owner_id: String(ab.owner_id ?? ""),
                subject: String(ab.subject ?? ""),
                esito: String(ab.esito ?? "ammissione") as SharedBet["esito"],
                sospensione: sosp,
                invite_code: String(ab.invite_code ?? ""),
                participants: parts,
                stances,
                created_at: String(ab.created_at ?? new Date().toISOString()),
                terminated_at: String((ab as any).terminated_at ?? ""),
                realized: String((ab as any).realized ?? ""),
              };
            })
          : [];
        setSharedBets(bets);
      }
    } catch (e) {
      const err = e as Error;
      setSharedError(err?.message || String(e));
    } finally {
      setLoadingShared(false);
    }
  }

  useEffect(() => {
    if (session?.userId) {
      refreshSharedBets();
    } else {
      setSharedBets([]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session?.userId]);

  // Aggiorna i contatori W/L quando la sessione cambia (incluso reload con sessione salvata)
  useEffect(() => {
    if (session?.userId) {
      refreshProfile();
    } else {
      setProfile(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session?.userId]);

  async function handleCreateShared(e: React.FormEvent) {
    e.preventDefault();
    if (!session?.userId) return;
    setSharedError("");
    setSharedInfo("");
    // Validazione nome/cognome
    if (!createNome.trim() || !createCognome.trim()) {
      setSharedError("Inserisci Nome e Cognome");
      return;
    }
    // Validazione sospensione
    if (createEsito === "sospensione") {
      const cleaned = sospensioneItems.filter((i) => i.subject.trim() !== "");
      if (cleaned.length === 0) {
        setSharedError("Aggiungi almeno una materia per la sospensione");
        return;
      }
    }
    try {
      const res = await fetch("/api/bets/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId: session.userId,
          subject: derivedSubject,
          esito: createEsito,
          stance: createStance,
          sospensione: createEsito === "sospensione" ? sospensioneItems.filter((i) => i.subject.trim() !== "") : [],
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        setSharedError(data?.message || "Creazione scommessa fallita");
        return;
      }
      setSharedInfo("Scommessa creata");
      setCreateEsito("ammissione");
      setCreateStance("favorevole");
      setSospensioneItems([{ subject: "", grade: 0 }]);
      setCreateNome("");
      setCreateCognome("");
      await refreshSharedBets();
    } catch (e) {
      const err = e as Error;
      setSharedError(err?.message || String(e));
    }
  }

  async function handleJoinBet(betId: string) {
    if (!session?.userId) return;
    setSharedError("");
    setSharedInfo("");
    try {
      const stance = joinStances[betId] || "favorevole";
      const res = await fetch("/api/bets/join", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: session.userId, betId, stance }),
      });
      const data = await res.json();
      if (!res.ok) {
        setSharedError(data?.message || "Unione alla scommessa fallita");
        return;
      }
      setSharedInfo("Ti sei unito alla scommessa");
      await refreshSharedBets();
      await refreshProfile();
    } catch (e) {
      const err = e as Error;
      setSharedError(err?.message || String(e));
    }
  }

  async function handleTerminateBet(betId: string, realized: boolean) {
    if (!session?.userId) return;
    setSharedError("");
    setSharedInfo("");
    try {
      const res = await fetch("/api/bets/terminate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: session.userId, betId, realized }),
      });
      const data = await res.json();
      if (!res.ok) {
        setSharedError(data?.message || "Terminazione fallita");
        return;
      }
      const winners = Array.isArray(data?.winners) ? data.winners.length : 0;
      const losers = Array.isArray(data?.losers) ? data.losers.length : 0;
      setSharedInfo(`Scommessa terminata. Realizzato: ${String(data.realized) === "true" ? "Sì" : "No"}. Vincitori: ${winners}, Perdenti: ${losers}.`);
      await refreshSharedBets();
      await refreshProfile();
    } catch (e) {
      const err = e as Error;
      setSharedError(err?.message || String(e));
    }
  }

  function handleLogout() {
    save(null);
  }
  // Rimosso: handler per scommesse locali
  async function handleDeleteBet(betId: string) {
    if (!session?.userId) return;
    setSharedError("");
    setSharedInfo("");
    try {
      const res = await fetch("/api/bets/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: session.userId, betId }),
      });
      const data = await res.json();
      if (!res.ok) {
        setSharedError(data?.message || "Eliminazione fallita");
        return;
      }
      setSharedInfo("Scommessa eliminata");
      await refreshSharedBets();
      await refreshProfile();
    } catch (e) {
      const err = e as Error;
      setSharedError(err?.message || String(e));
    }
  }

  async function handleUpdateAccount(e: React.FormEvent) {
    e.preventDefault();
    setAccountError("");
    setAccountInfo("");
    if (!session?.userId) return;
    if (!editUsername.trim() && !editPassword.trim()) {
      setAccountError("Inserisci almeno username o password");
      return;
    }
    try {
      const payload: Record<string, unknown> = { userId: session.userId };
      if (editUsername.trim()) payload["newUsername"] = editUsername.trim();
      if (editPassword.trim()) payload["newPassword"] = editPassword.trim();
      const res = await fetch("/api/users/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        setAccountError(data?.message || "Aggiornamento fallito");
        return;
      }
      const updatedUsername = String(data?.user?.username ?? session.username);
      // Aggiorna sessione locale
      save({ userId: session.userId, username: updatedUsername, is_admin: session.is_admin });
      setAccountInfo("Credenziali aggiornate");
      setEditPassword("");
      setIsEditingAccount(false);
    } catch (e) {
      const err = e as Error;
      setAccountError(err?.message || String(e));
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-b from-background to-muted/30">
      <div className="mx-auto max-w-2xl p-6">
        <header className="flex items-center justify-between mb-6">
          <h1 className="text-2xl font-semibold tracking-tight">Fantadebito</h1>
          {session ? (
            <div className="flex items-center gap-3">
              <button
                className="text-sm opacity-80 underline-offset-4 hover:underline"
                onClick={() => {
                  setIsEditingAccount(true);
                  setEditUsername(session.username);
                  setEditPassword("");
                  setAccountError("");
                  setAccountInfo("");
                }}
                title="Modifica username e password"
              >
                Ciao, {session.username}
              </button>
              {session.is_admin ? (
                <span className="text-[11px] px-2 py-0.5 rounded border opacity-80">Admin</span>
              ) : null}
              {profile ? (
                <span className="text-xs opacity-70">W {profile.wins} / L {profile.losses}</span>
              ) : null}
              <button className="px-3 py-1 rounded border hover:bg-muted transition" onClick={handleLogout}>
                Esci
              </button>
            </div>
          ) : null}
        </header>

        {session && isEditingAccount ? (
          <section className="bg-card border rounded-xl shadow-sm mb-6">
            <div className="p-4 space-y-4">
              <div className="space-y-1">
                <h2 className="text-base font-semibold">Modifica account</h2>
                <p className="text-xs opacity-70">Aggiorna il tuo username e/o password.</p>
              </div>
              <form className="grid gap-3" onSubmit={handleUpdateAccount}>
                <label className="grid gap-1">
                  <span className="text-sm">Nuovo username</span>
                  <input
                    type="text"
                    className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                    value={editUsername}
                    onChange={(e) => setEditUsername(e.target.value)}
                    placeholder="Nuovo username"
                  />
                </label>
                <label className="grid gap-1">
                  <span className="text-sm">Nuova password</span>
                  <input
                    type="password"
                    className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                    value={editPassword}
                    onChange={(e) => setEditPassword(e.target.value)}
                    placeholder="Nuova password"
                  />
                </label>
                {accountError ? <div className="text-red-600 text-sm">{accountError}</div> : null}
                {accountInfo ? <div className="text-green-700 text-sm">{accountInfo}</div> : null}
                <div className="flex items-center gap-3">
                  <button className="px-4 py-2 rounded bg-foreground text-background hover:opacity-90 transition" type="submit">Salva</button>
                  <button
                    type="button"
                    className="text-sm underline"
                    onClick={() => {
                      setIsEditingAccount(false);
                      setAccountError("");
                      setAccountInfo("");
                    }}
                  >
                    Annulla
                  </button>
                </div>
              </form>
            </div>
          </section>
        ) : null}

      {!session ? (
        <section className="bg-card border rounded-xl shadow-sm">
          <div className="p-6 space-y-5">
            <div className="space-y-1">
              <h2 className="text-xl font-semibold">Accedi</h2>
              <p className="text-sm opacity-80">Inserisci le tue credenziali per iniziare a scommettere.</p>
            </div>
            <form className="grid gap-4" onSubmit={handleLogin}>
              <label className="grid gap-1">
                <span className="text-sm">Username</span>
                <input
                  type="text"
                  required
                  className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                />
              </label>
              <label className="grid gap-1">
                <span className="text-sm">Password</span>
                <input
                  type="password"
                  required
                  className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                />
              </label>
              {error ? <div className="text-red-600 text-sm">{error}</div> : null}
              <div className="flex items-center gap-3">
                <button className="px-4 py-2 rounded bg-foreground text-background hover:opacity-90 transition" type="submit">
                  {isRegister ? "Registrati" : "Accedi"}
                </button>
                <button type="button" className="text-sm underline" onClick={() => setIsRegister(!isRegister)}>
                  {isRegister ? "Hai già un account? Accedi" : "Non hai un account? Registrati"}
                </button>
              </div>
            </form>
          </div>
        </section>
      ) : (
        <>
          {sharedError ? <p className="text-red-600 text-sm mb-4">{sharedError}</p> : null}
          {sharedInfo ? <p className="text-green-600 text-sm mb-4">{sharedInfo}</p> : null}

          <section className="bg-card border rounded-xl shadow-sm mb-6">
            <div className="p-6 space-y-4">
              <div className="space-y-1">
                <h2 className="text-xl font-semibold">Crea scommessa</h2>
                <p className="text-sm opacity-80">Definisci l&apos;oggetto e l&apos;esito desiderato.</p>
              </div>
              <form onSubmit={handleCreateShared} className="grid gap-3">
                <div className="grid gap-3 grid-cols-1 sm:grid-cols-2">
                  <label className="grid gap-1">
                    <span className="text-sm">Nome</span>
                    <input
                      type="text"
                      className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                      value={createNome}
                      onChange={(e) => setCreateNome(e.target.value)}
                      placeholder="Mario"
                    />
                  </label>
                  <label className="grid gap-1">
                    <span className="text-sm">Cognome</span>
                    <input
                      type="text"
                      className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                      value={createCognome}
                      onChange={(e) => setCreateCognome(e.target.value)}
                      placeholder="Rossi"
                    />
                  </label>
                </div>
                <label className="grid gap-1">
                  <span className="text-sm">Oggetto (derivato dalle iniziali)</span>
                  <input
                    type="text"
                    readOnly
                    className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20 bg-muted"
                    value={derivedSubject}
                  />
                </label>
                <label className="grid gap-1">
                  <span className="text-sm">Posizione</span>
                  <select
                    className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                    value={createStance}
                    onChange={(e) => setCreateStance(e.target.value)}
                  >
                    <option value="favorevole">Favorevole</option>
                    <option value="contrario">Contrario</option>
                  </select>
                </label>
                <label className="grid gap-1">
                  <span className="text-sm">Esito dell&apos;anno</span>
                  <select
                    className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                    value={createEsito}
                    onChange={(e) => setCreateEsito(e.target.value)}
                  >
                    <option value="ammissione">Ammissione all'anno successivo</option>
                    <option value="sospensione">Sospensione in giudizio</option>
                    <option value="non_ammissione">Non ammissione</option>
                  </select>
                </label>
                {createEsito === "sospensione" ? (
                  <div className="grid gap-2 border rounded p-2">
                    <div className="text-sm font-medium">Materie e voti in sospensione</div>
                    {sospensioneItems.map((item, idx) => (
                      <div key={idx} className="grid grid-cols-[1fr_120px_auto] gap-2 items-center">
                        <input
                          type="text"
                          placeholder="Materia"
                          className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                          value={item.subject}
                          onChange={(e) => {
                            const next = [...sospensioneItems];
                            next[idx] = { ...next[idx], subject: e.target.value };
                            setSospensioneItems(next);
                          }}
                        />
                        <input
                          type="number"
                          min={0}
                          max={30}
                          placeholder="Voto"
                          className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-foreground/20"
                          value={item.grade}
                          onChange={(e) => {
                            const next = [...sospensioneItems];
                            next[idx] = { ...next[idx], grade: Number(e.target.value) };
                            setSospensioneItems(next);
                          }}
                        />
                        <button
                          type="button"
                          className="text-sm px-2 py-1 rounded border hover:bg-muted transition"
                          onClick={() => {
                            const next = sospensioneItems.filter((_, i) => i !== idx);
                            setSospensioneItems(next.length ? next : [{ subject: "", grade: 0 }]);
                          }}
                        >
                          Rimuovi
                        </button>
                      </div>
                    ))}
                    <button
                      type="button"
                      className="text-sm px-2 py-1 rounded border hover:bg-muted transition w-fit"
                      onClick={() => setSospensioneItems([...sospensioneItems, { subject: "", grade: 0 }])}
                    >
                      Aggiungi materia
                    </button>
                  </div>
                ) : null}
                <button className="px-3 py-2 rounded border hover:bg-muted transition w-fit" type="submit">Crea scommessa</button>
              </form>
            </div>
          </section>

          {/* Sezione join per codice rimossa: le scommesse sono pubbliche e si gestiscono per-bet */}

          <section className="bg-card border rounded-xl shadow-sm mb-6">
            <div className="p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="space-y-1">
                  <h2 className="text-xl font-semibold">Scommesse pubbliche</h2>
                  <p className="text-sm opacity-80">Partecipa e gestisci quelle che ti interessano.</p>
                </div>
                <button className="text-sm px-2 py-1 rounded border hover:bg-muted transition" onClick={refreshSharedBets} disabled={loadingShared}>
                  {loadingShared ? "Carico..." : "Aggiorna"}
                </button>
              </div>
              <ul className="mt-2 space-y-2">
                {sharedBets.length === 0 ? (
                  <li className="text-sm opacity-70">Nessuna scommessa. Crea o unisciti.</li>
                ) : (
                  sharedBets.map((b) => {
                    const myStance = Array.isArray(b.stances) ? b.stances.find((s) => s.userId === (session?.userId ?? ""))?.stance : undefined;
                    return (
                      <li key={b.id} className="border rounded px-3 py-2">
                        <div className="flex items-center justify-between">
                          <span className="font-medium">{b.subject}</span>
                           <div className="flex items-center gap-2">
                             {(b.owner_id === session?.userId || session?.is_admin) ? (
                               <>
                                 <button
                                   className="text-xs px-2 py-1 rounded border hover:bg-muted transition"
                                   onClick={() => handleTerminateBet(b.id, true)}
                                   disabled={Boolean(b.terminated_at)}
                                 >
                                   Termina: Realizzato
                                 </button>
                                 <button
                                   className="text-xs px-2 py-1 rounded border hover:bg-muted transition"
                                   onClick={() => handleTerminateBet(b.id, false)}
                                   disabled={Boolean(b.terminated_at)}
                                 >
                                   Termina: Non realizzato
                                 </button>
                               </>
                             ) : null}
                             {session?.is_admin ? (
                               <button
                                 className="text-xs px-2 py-1 rounded border hover:bg-red-50 hover:text-red-700 transition"
                                 onClick={() => handleDeleteBet(b.id)}
                               >
                                 Elimina
                               </button>
                             ) : null}
                           </div>
                        </div>
                        <div className="text-sm opacity-80">Partecipanti: {Array.isArray(b.participants) ? b.participants.length : 0}</div>
                        <div className="text-sm">
                          Esito: {b.esito === "ammissione" && "Ammissione"}
                          {b.esito === "sospensione" && (
                            <span>
                              Sospensione in giudizio — {Array.isArray(b.sospensione) && b.sospensione.length > 0 ? b.sospensione.map((s) => `${s.subject}:${s.grade}`).join(", ") : "nessuna materia"}
                            </span>
                          )}
                          {b.esito === "non_ammissione" && "Non ammissione"}
                        </div>
                        <div className="text-sm">La tua posizione: {myStance ? (myStance === "favorevole" ? "Favorevole" : "Contrario") : "non impostata"}</div>
                        {!b.terminated_at ? (
                          <div className="mt-2 flex items-center gap-2">
                            {myStance ? (
                              <span className="text-xs opacity-70">Hai già aderito</span>
                            ) : (
                              <>
                                <select
                                  className="text-xs border rounded px-2 py-1"
                                  value={joinStances[b.id] || "favorevole"}
                                  onChange={(e) => setJoinStances({ ...joinStances, [b.id]: e.target.value as any })}
                                >
                                  <option value="favorevole">Favorevole</option>
                                  <option value="contrario">Contrario</option>
                                </select>
                                <button
                                  className="text-xs px-2 py-1 rounded border hover:bg-muted transition"
                                  onClick={() => handleJoinBet(b.id)}
                                  disabled={!session?.userId}
                                >
                                  Unisciti
                                </button>
                              </>
                            )}
                          </div>
                        ) : null}
                        {b.terminated_at ? (
                          <div className="text-xs opacity-70">
                            Terminata: {new Date(b.terminated_at).toLocaleString()} — Realizzato: {String(b.realized) === "true" ? "Sì" : "No"}
                          </div>
                        ) : null}
                        <div className="text-xs opacity-60">Creato: {new Date(b.created_at).toLocaleString()}</div>
                      </li>
                    );
                  })
                )}
              </ul>
            </div>
          </section>

          {/* Sezione scommesse locali rimossa. Le scommesse sono solo condivise. */}
          </>
        )}

        <footer className="mt-10 text-center text-xs opacity-60">
          Fantadebito — scommesse condivise.
        </footer>
      </div>
    </div>
  );
}