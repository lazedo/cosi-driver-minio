#!/usr/bin/env bash
# MOVE a COSI bucket to another MinIO. The name goes with it and the source
# stops existing -- that is what migrating means, and it is what makes this
# different from a copy.
#
# THE WORKLOAD IS NEVER EDITED. It ends bound to a claim with the SAME name, an
# access with the same name, and therefore a BucketInfo secret with the same
# name -- so no Deployment, no manifest and no repo file changes. A migration
# that leaves the consumer pointing somewhere new has moved the problem instead
# of the data.
#
# WHY THERE IS A STAGING BUCKET
#
# The bucket's name comes from the claim's name, and two claims cannot share a
# name in one namespace. So the final name is not available until the old claim
# is gone -- and deleting it, with deletionPolicy: Delete, destroys the source.
# The data therefore has to be somewhere else first.
#
# Staging lives ON THE DESTINATION, not on the source and not off-cluster: once
# the old claim is deleted the remaining move is inside one MinIO, which is fast
# and cannot be interrupted by the source no longer existing.
#
#   1. staging claim on the target class          (nothing moves yet)
#   2. copy source -> staging, workload UP        (the bulk, no downtime)
#   3. stop the workload                          (downtime starts)
#   4. copy the tail, verify staging == source    (before anything is destroyed)
#   5. DELETE the old claim -- THE SOURCE IS GONE (the name is now free)
#   6. recreate claim+access with the SAME names on the target class
#   7. copy staging -> final, verify              (inside one MinIO)
#   8. drop staging, restart the workload         (downtime ends)
#
# STEP 5 IS IRREVERSIBLE. Between it and step 7 the only copy of the data is the
# staging bucket, and that is inherent: the name cannot be freed without
# destroying what holds it. Step 4 verifies objects AND bytes first, and the
# script refuses to reach step 5 unless they match exactly.
#
# WHAT MAKES THIS SAFE FOR THIS CONSUMER: couch_doc_attachments stores
# object_key and nothing about location -- no bucket, no endpoint. Keys are
# preserved, so no rows are rewritten. A consumer that persisted its endpoint
# would need a different script.
#
# Nothing is asked of whoever runs this. Verification is a comparison the script
# makes, not two numbers it prints for a person to judge: a prompt that only
# appears when everything is fine teaches people to answer it without reading.
#
# Usage:
#   migrate-bucket.sh --claim crdb-sso/couch-attachments \
#                     --to-class minio-myminio4 \
#                     --workload deploy/couch-crdb-sink \
#                     [--context kind-west] [--dry-run]
set -euo pipefail

CTX="${CTX:-kind-west}"
DRY=""
CLAIM_REF="" TO_CLASS="" WORKLOAD=""
MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"

die()  { echo "erro: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --claim)    CLAIM_REF="$2"; shift 2;;
    --to-class) TO_CLASS="$2"; shift 2;;
    --workload) WORKLOAD="$2"; shift 2;;
    --context)  CTX="$2"; shift 2;;
    --dry-run)  DRY=1; shift;;
    *) die "argumento desconhecido: $1";;
  esac
done

[ -n "$CLAIM_REF" ] || die "--claim <ns>/<nome> é obrigatório"
[ -n "$TO_CLASS" ]  || die "--to-class é obrigatório"
[ -n "$WORKLOAD" ]  || die "--workload é obrigatório (ex: deploy/couch-crdb-sink)"

NS="${CLAIM_REF%%/*}"
CLAIM="${CLAIM_REF##*/}"
STAGING="${CLAIM}-staging"
K="kubectl --context $CTX"
run() { if [ -n "$DRY" ]; then echo "    [dry-run] $*"; else "$@"; fi; }

# --- 0. o que existe hoje ---------------------------------------------------
step "estado inicial"
$K -n "$NS" get bucketclaim "$CLAIM" >/dev/null 2>&1 || die "não há BucketClaim $CLAIM_REF"
$K get bucketclass "$TO_CLASS" >/dev/null 2>&1 || die "não há BucketClass $TO_CLASS"
$K get bucketaccessclass "$TO_CLASS" >/dev/null 2>&1 \
  || die "não há BucketAccessClass $TO_CLASS -- o par tem de existir, senão as credenciais saem de um driver e o bucket de outro"

SRC_SECRET="$($K -n "$NS" get bucketaccess "$CLAIM" -o jsonpath='{.spec.credentialsSecretName}' 2>/dev/null)"
[ -n "$SRC_SECRET" ] || die "não encontrei o BucketAccess $CLAIM_REF"
FROM_CLASS="$($K -n "$NS" get bucketclaim "$CLAIM" -o jsonpath='{.spec.bucketClassName}')"
[ "$FROM_CLASS" != "$TO_CLASS" ] || die "o claim já está na classe $TO_CLASS -- nada a mover"

# O nome do secret é o que prende o workload. Recriar o access com o mesmo nome
# devolve um secret com o mesmo nome, e é por isso que nada no Deployment muda.
echo "    claim   : $CLAIM_REF   $FROM_CLASS -> $TO_CLASS"
echo "    secret  : $SRC_SECRET  (recriado com o MESMO nome; o workload não é tocado)"
echo "    staging : $NS/$STAGING (temporário, no destino)"

DELPOL="$($K get bucketclass "$FROM_CLASS" -o jsonpath='{.deletionPolicy}' 2>/dev/null)"
echo "    classe de origem: deletionPolicy=$DELPOL"

# --- helpers ----------------------------------------------------------------
#
# O corpo do mc vive num ConfigMap montado como ficheiro. Embutido no YAML do
# Job, partiu-se duas vezes: `sed: command not found` (a imagem só tem mc e o
# shell) e um `$1` comido pelas camadas de escape -- este ficheiro atravessa um
# heredoc, YAML e `sh -c`. Com um ficheiro há uma camada: o que se escreve é o
# que corre.
#
# mc corre no cluster: os endpoints são serviços internos e as credenciais são
# Secrets. Montá-los é mais simples e mais seguro do que exportá-los.
MC_SCRIPT="$(mktemp -t cosi-mc)"
trap 'rm -f "$MC_SCRIPT"' EXIT
cat > "$MC_SCRIPT" <<'MCEOF'
#!/bin/sh
set -e
# Só expansão de parâmetros: a imagem do mc não tem sed, awk nem jq.
field() { t=$(cat "$1"); t=${t#*\"$2\":\"}; printf '%s' "${t%%\"*}"; }
num()   { t=${1#*\"$2\":}; printf '%s' "${t%%,*}"; }

a_ep=$(field /a/BucketInfo endpoint);        b_ep=$(field /b/BucketInfo endpoint)
a_ak=$(field /a/BucketInfo accessKeyID);     b_ak=$(field /b/BucketInfo accessKeyID)
a_sk=$(field /a/BucketInfo accessSecretKey); b_sk=$(field /b/BucketInfo accessSecretKey)
a_b=$(field /a/BucketInfo bucketName);       b_b=$(field /b/BucketInfo bucketName)
[ -n "$a_ep" ] && [ -n "$a_ak" ] && [ -n "$a_b" ] || { echo "BucketInfo A ilegivel" >&2; exit 1; }
[ -n "$b_ep" ] && [ -n "$b_ak" ] && [ -n "$b_b" ] || { echo "BucketInfo B ilegivel" >&2; exit 1; }
mc alias set a "$a_ep" "$a_ak" "$a_sk" --api S3v4 >/dev/null
mc alias set b "$b_ep" "$b_ak" "$b_sk" --api S3v4 >/dev/null

case "$1" in
  mirror)
    echo "mirror $a_b -> $b_b ${2:-}"
    # --exit-on-error: sem ele o mc imprime um erro por objecto e sai com 0.
    # Aconteceu: "Overwrite not allowed" em todos os 106 objectos, o job deu
    # Complete, e o script seguiu para a verificacao como se tivesse copiado.
    mc mirror --preserve --exit-on-error ${2:-} "a/$a_b" "b/$b_b"
    ;;
  verify)
    echo "A_OBJECTS=$(mc ls --recursive "a/$a_b" | wc -l | tr -d ' ')"
    echo "B_OBJECTS=$(mc ls --recursive "b/$b_b" | wc -l | tr -d ' ')"
    echo "A_BYTES=$(num "$(mc du --json "a/$a_b")" size)"
    echo "B_BYTES=$(num "$(mc du --json "b/$b_b")" size)"
    ;;
  count)
    echo "A_OBJECTS=$(mc ls --recursive "a/$a_b" | wc -l | tr -d ' ')"
    ;;
  rb)
    # Esvaziar e remover. --force porque um bucket com objectos nao se apaga, e
    # o ponto e nao deixar dados atras.
    echo "rb $a_b"
    mc rb --force "a/$a_b"
    ;;
  *) echo "modo desconhecido: $1" >&2; exit 1;;
esac
MCEOF

# mc_job <modo> <secretA> <secretB> [arg]
mc_job() {
  local mode="$1" seca="$2" secb="$3" arg="${4:-}"
  local job="cosi-move-${mode}-$(date +%s)"
  $K -n "$NS" create configmap "$job" --from-file=run.sh="$MC_SCRIPT" >/dev/null
  cat <<YAML | $K -n "$NS" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: $job }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mc
          image: $MC_IMAGE
          command: ["/bin/sh", "/mc/run.sh", "$mode", "$arg"]
          volumeMounts:
            - { name: mc, mountPath: /mc, readOnly: true }
            - { name: a, mountPath: /a, readOnly: true }
            - { name: b, mountPath: /b, readOnly: true }
      volumes:
        - { name: mc, configMap: { name: $job } }
        - { name: a, secret: { secretName: $seca } }
        - { name: b, secret: { secretName: $secb } }
YAML
  # Complete OU Failed. `wait --for=condition=complete` sozinho espera o timeout
  # inteiro por um job que ja falhou -- horas de script preso por um erro
  # ocorrido em dois segundos, que foi o que este script fez a primeira.
  while :; do
    c=$($K -n "$NS" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null)
    f=$($K -n "$NS" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null)
    [ "$c" = "True" ] && break
    [ "$f" = "True" ] && { $K -n "$NS" logs "job/$job" 2>&1 | tail -20 >&2
                           $K -n "$NS" delete job "$job" cm "$job" >/dev/null 2>&1 || true
                           die "job $mode falhou"; }
    sleep 5
  done
  $K -n "$NS" logs "job/$job"
  $K -n "$NS" delete job "$job" cm "$job" >/dev/null 2>&1 || true
}

# equal_or_die compara os dois lados. Objectos E bytes: a contagem sozinha nao
# apanha um objecto truncado, que e a falha que uma migracao nao pode deixar
# passar -- sobretudo antes de um passo que destroi a origem.
equal_or_die() {
  local seca="$1" secb="$2" what="$3"
  eval "$(mc_job verify "$seca" "$secb" | grep -E '^[AB]_(OBJECTS|BYTES)=')"
  echo "    objectos: A=${A_OBJECTS:-?} B=${B_OBJECTS:-?}   bytes: A=${A_BYTES:-?} B=${B_BYTES:-?}"
  [ -n "${A_OBJECTS:-}" ] && [ "${A_OBJECTS}" = "${B_OBJECTS:-}" ] || die "$what: contagens diferentes"
  [ -n "${A_BYTES:-}" ]   && [ "${A_BYTES}"   = "${B_BYTES:-}"   ] || die "$what: bytes diferentes"
  echo "    igual dos dois lados"
}

claim_and_access() {  # $1=nome  $2=classe  $3=secret
  cat <<YAML | $K apply -f - >/dev/null
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClaim
metadata: { name: $1, namespace: $NS }
spec:
  bucketClassName: $2
  protocols: [S3]
---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccess
metadata: { name: $1, namespace: $NS }
spec:
  bucketAccessClassName: $2
  bucketClaimName: $1
  credentialsSecretName: $3
  protocol: S3
YAML
  for _ in $(seq 1 60); do
    r="$($K -n "$NS" get bucketclaim "$1" -o jsonpath='{.status.bucketReady}' 2>/dev/null || true)"
    g="$($K -n "$NS" get bucketaccess "$1" -o jsonpath='{.status.accessGranted}' 2>/dev/null || true)"
    [ "$r" = "true" ] && [ "$g" = "true" ] && return 0
    sleep 5
  done
  die "$1 nao ficou pronto (ver eventos do BucketAccess/$1)"
}

# --- 1. staging no destino --------------------------------------------------
step "1/8 bucket de staging no destino"
if [ -n "$DRY" ]; then echo "    [dry-run] claim+access $STAGING em $TO_CLASS"
else claim_and_access "$STAGING" "$TO_CLASS" "${STAGING}-bucketinfo"; echo "    pronto"; fi

# --- 2. copia a quente ------------------------------------------------------
step "2/8 copiar origem -> staging, com o workload A CORRER"
[ -n "$DRY" ] && echo "    [dry-run] mirror" || mc_job mirror "$SRC_SECRET" "${STAGING}-bucketinfo"

# --- 3. parar ---------------------------------------------------------------
step "3/8 parar $WORKLOAD -- comeca a indisponibilidade"
REPLICAS="$($K -n "$NS" get "$WORKLOAD" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 1)"
run $K -n "$NS" scale "$WORKLOAD" --replicas=0
[ -n "$DRY" ] || $K -n "$NS" rollout status "$WORKLOAD" --timeout=180s >/dev/null 2>&1 || true

# --- 4. cauda + verificacao ANTES de destruir -------------------------------
step "4/8 copiar a cauda e verificar (antes de destruir seja o que for)"
if [ -z "$DRY" ]; then
  mc_job mirror "$SRC_SECRET" "${STAGING}-bucketinfo" "--remove"
  equal_or_die "$SRC_SECRET" "${STAGING}-bucketinfo" "origem vs staging"
fi

# --- 5. destruir a origem ---------------------------------------------------
step "5/8 apagar o claim antigo -- A ORIGEM DEIXA DE EXISTIR (deletionPolicy=$DELPOL)"
echo "    a partir daqui e ate ao passo 7 os dados vivem SO no staging"
run $K -n "$NS" delete bucketaccess "$CLAIM" --wait=true
run $K -n "$NS" delete bucketclaim "$CLAIM" --wait=true
if [ -z "$DRY" ]; then
  # ESPERAR PELO Bucket, NAO PELO CLAIM. O que prende o nome e o objecto
  # cluster-scoped: o claim some primeiro e o Bucket fica a terminar atras dele,
  # e um claim novo criado nessa janela ve um Bucket da classe ANTIGA e recusa
  # -- "ClassMismatch: adoption never crosses backend", correctamente. Foi
  # exactamente assim que esta migracao rebentou no passo 6 a primeira vez, com
  # a origem ja destruida.
  for _ in $(seq 1 120); do
    $K -n "$NS" get bucketclaim "$CLAIM" >/dev/null 2>&1 && { sleep 5; continue; }
    $K get bucket "$CLAIM" >/dev/null 2>&1 || break
    sleep 5
  done
  $K -n "$NS" get bucketclaim "$CLAIM" >/dev/null 2>&1 && die "o claim antigo nao desapareceu -- o nome continua ocupado"
  $K get bucket "$CLAIM" >/dev/null 2>&1 && die "o Bucket cluster-scoped $CLAIM ainda existe -- criar o claim agora daria ClassMismatch"
fi
echo "    nome libertado (claim e Bucket)"

# --- 6. recriar com o MESMO nome no destino ---------------------------------
step "6/8 recriar $CLAIM (mesmo nome, mesmo secret) na classe $TO_CLASS"
if [ -n "$DRY" ]; then echo "    [dry-run] claim+access $CLAIM em $TO_CLASS com secret $SRC_SECRET"
else claim_and_access "$CLAIM" "$TO_CLASS" "$SRC_SECRET"; echo "    pronto"; fi

# --- 7. staging -> final ----------------------------------------------------
step "7/8 copiar staging -> $CLAIM (dentro do mesmo MinIO) e verificar"
if [ -z "$DRY" ]; then
  # O DESTINO RECRIADO TEM DE ESTAR VAZIO, e isto nao e zelo.
  #
  # Com deletionPolicy: Retain o bucket sobrevive ao claim, COM os dados dentro.
  # Um claim novo com o mesmo nome herda esse conteudo -- e foi o que aconteceu
  # numa ida-e-volta: o bucket de destino ainda tinha a copia da migracao
  # anterior, o mirror recusou sobrepor tudo, e a verificacao passou na mesma
  # porque contagem e bytes batiam certo... contra dados velhos.
  #
  # Se o conteudo antigo fosse DIFERENTE, a migracao teria "sucedido" deixando
  # o consumidor a ler outra coisa. Um destino nao-vazio nao e um detalhe de
  # limpeza, e a diferenca entre mover dados e parecer que se moveu.
  eval "$(mc_job count "$SRC_SECRET" "$SRC_SECRET" | grep '^A_OBJECTS=')"
  if [ "${A_OBJECTS:-0}" != "0" ]; then
    echo "    o destino tem ${A_OBJECTS} objectos de uma vida anterior (Retain); a esvaziar"
    mc_job rb "$SRC_SECRET" "$SRC_SECRET" >/dev/null
    # rb apaga o bucket; recriar o claim devolve-o vazio.
    $K -n "$NS" delete bucketaccess "$CLAIM" --wait=true >/dev/null
    $K -n "$NS" delete bucketclaim "$CLAIM" --wait=true >/dev/null
    for _ in $(seq 1 120); do
      $K -n "$NS" get bucketclaim "$CLAIM" >/dev/null 2>&1 && { sleep 5; continue; }
      $K get bucket "$CLAIM" >/dev/null 2>&1 || break
      sleep 5
    done
    claim_and_access "$CLAIM" "$TO_CLASS" "$SRC_SECRET"
    eval "$(mc_job count "$SRC_SECRET" "$SRC_SECRET" | grep '^A_OBJECTS=')"
    [ "${A_OBJECTS:-1}" = "0" ] || die "o destino continua com ${A_OBJECTS} objectos -- nao copio para cima deles"
  fi
  echo "    destino vazio, como tem de estar"
  mc_job mirror "${STAGING}-bucketinfo" "$SRC_SECRET"
  equal_or_die "${STAGING}-bucketinfo" "$SRC_SECRET" "staging vs final"
fi

# --- 8. limpar e arrancar ---------------------------------------------------
step "8/8 remover o staging e arrancar $WORKLOAD"
# APAGAR O CLAIM PODE NAO APAGAR O BUCKET. A classe de destino aqui tem
# deletionPolicy=Retain (as classes criadas pelo discover tem), e Retain
# significa exactamente isto: os objectos COSI vao-se e o bucket fica no MinIO,
# com os dados dentro. Um staging que sobrevive a migracao e uma segunda copia
# dos dados que ninguem sabe que existe, portanto e esvaziado e removido AQUI,
# pelo S3, antes de os objectos COSI desaparecerem -- depois deles nao ha
# credencial para lhe chegar.
if [ -z "$DRY" ]; then
  DELPOL_DST="$($K get bucketclass "$TO_CLASS" -o jsonpath='{.deletionPolicy}' 2>/dev/null)"
  echo "    classe de destino: deletionPolicy=$DELPOL_DST"
  if [ "$DELPOL_DST" != "Delete" ]; then
    echo "    a remover o bucket de staging pelo S3 (a politica nao o faz)"
    mc_job rb "${STAGING}-bucketinfo" "${STAGING}-bucketinfo" || true
  fi
fi
run $K -n "$NS" delete bucketaccess "$STAGING" --wait=false
run $K -n "$NS" delete bucketclaim "$STAGING" --wait=false
run $K -n "$NS" scale "$WORKLOAD" --replicas="$REPLICAS"
[ -n "$DRY" ] || $K -n "$NS" rollout status "$WORKLOAD" --timeout=300s

cat <<TXT

    Concluido. O claim $CLAIM_REF existe com o mesmo nome, na classe $TO_CLASS,
    e o secret $SRC_SECRET foi recriado com o mesmo nome -- o Deployment, os
    manifestos e o repo nao foram tocados, porque nada neles mudou.

    A origem ja nao existe.
TXT
