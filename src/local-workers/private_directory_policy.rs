//! Same current-user/SYSTEM/Administrators predicate as the original ACL verifier.
pub(super) const FULL_CONTROL: u32 = 0x001f_01ff;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Rule {
    pub sid: String,
    pub allow: bool,
    pub rights: u32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    pub protected: bool,
    pub rules: Vec<Rule>,
}
pub(super) trait Security {
    fn read(&self) -> Result<Snapshot, String>;
    fn apply(&self) -> Result<(), String>;
}
pub(super) fn is_private(acl: &Snapshot, allowed: &[String]) -> bool {
    acl.protected
        && acl
            .rules
            .iter()
            .all(|rule| rule.allow && allowed.contains(&rule.sid))
        && allowed.iter().all(|sid| {
            acl.rules.iter().any(|rule| {
                &rule.sid == sid && rule.allow && rule.rights & FULL_CONTROL == FULL_CONTROL
            })
        })
}
pub(super) fn protect(security: &impl Security, allowed: &[String]) -> Result<(), String> {
    if is_private(&security.read()?, allowed) {
        return Ok(());
    }
    security.apply()?;
    if !is_private(&security.read()?, allowed) {
        return Err("failed to verify native private directory ACL".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    fn allowed() -> Vec<String> {
        ["S-1-5-21-1-2-3-1001", "S-1-5-18", "S-1-5-32-544"]
            .map(str::to_owned)
            .to_vec()
    }
    fn safe() -> Snapshot {
        Snapshot {
            protected: true,
            rules: allowed()
                .into_iter()
                .map(|sid| Rule {
                    sid,
                    allow: true,
                    rights: FULL_CONTROL,
                })
                .collect(),
        }
    }
    struct Fake {
        reads: RefCell<VecDeque<Result<Snapshot, String>>>,
        writes: Cell<usize>,
        rejected: bool,
    }
    impl Security for Fake {
        fn read(&self) -> Result<Snapshot, String> {
            self.reads
                .borrow_mut()
                .pop_front()
                .expect("bounded read plan")
        }
        fn apply(&self) -> Result<(), String> {
            self.writes.set(self.writes.get() + 1);
            if self.rejected {
                Err("native access denied".into())
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn already_private_is_a_read_only_noop_even_with_extra_allowed_partial_rules() {
        let mut original = safe();
        original.rules.push(Rule {
            sid: allowed()[0].clone(),
            allow: true,
            rights: 1,
        });
        let ops = Fake {
            reads: RefCell::new(VecDeque::from([Ok(original)])),
            writes: Cell::new(0),
            rejected: false,
        };
        protect(&ops, &allowed()).unwrap();
        assert_eq!(ops.writes.get(), 0);
        assert!(ops.reads.borrow().is_empty());
    }
    #[test]
    fn exact_original_three_sid_full_control_and_protection_guards_are_preserved() {
        assert!(is_private(&safe(), &allowed()));
        for kind in ["unprotected", "foreign", "deny", "missing", "partial-union"] {
            let mut invalid = safe();
            match kind {
                "unprotected" => invalid.protected = false,
                "foreign" => invalid.rules.push(Rule {
                    sid: "S-1-5-11".into(),
                    allow: true,
                    rights: 1,
                }),
                "deny" => invalid.rules[0].allow = false,
                "missing" => {
                    invalid.rules.pop();
                }
                _ => {
                    invalid.rules[0].rights = 1;
                    invalid.rules.push(Rule {
                        sid: allowed()[0].clone(),
                        allow: true,
                        rights: FULL_CONTROL ^ 1,
                    });
                }
            }
            assert!(!is_private(&invalid, &allowed()), "{kind}");
            let ops = Fake {
                reads: RefCell::new(VecDeque::from([Ok(invalid), Ok(safe())])),
                writes: Cell::new(0),
                rejected: false,
            };
            protect(&ops, &allowed()).unwrap();
            assert_eq!(ops.writes.get(), 1, "{kind}");
        }
    }
    #[test]
    fn read_errors_write_rejection_and_tampered_readback_never_report_success() {
        let mut unsafe_acl = safe();
        unsafe_acl.protected = false;
        for (reads, rejected, writes) in [
            (vec![Err("native read denied".into())], false, 0),
            (vec![Ok(unsafe_acl.clone())], true, 1),
            (
                vec![Ok(unsafe_acl.clone()), Err("native verify denied".into())],
                false,
                1,
            ),
            (vec![Ok(unsafe_acl.clone()), Ok(unsafe_acl)], false, 1),
        ] {
            let ops = Fake {
                reads: RefCell::new(reads.into()),
                writes: Cell::new(0),
                rejected,
            };
            assert!(protect(&ops, &allowed()).is_err());
            assert_eq!(ops.writes.get(), writes);
        }
    }
}
