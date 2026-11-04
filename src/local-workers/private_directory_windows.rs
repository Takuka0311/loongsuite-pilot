//! Native Windows ACL implementation, executed in the bounded Pilot helper child.
use super::acl_policy::{Rule, Security, Snapshot};
use std::ffi::c_void;
use std::ptr::{addr_of, null, null_mut};
use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SetSecurityInfo, SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetSecurityDescriptorLength,
    GetTokenInformation, IsValidAcl, IsValidSid, TokenUser, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
    DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
    SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx, FILE_ATTRIBUTE_DIRECTORY,
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

struct Handle(HANDLE);
impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: every Handle is constructed from one owned successful open.
        unsafe {
            CloseHandle(self.0);
        }
    }
}
struct LocalMemory(*mut c_void);
impl Drop for LocalMemory {
    fn drop(&mut self) {
        // SAFETY: this pointer is one LocalAlloc allocation returned by Win32.
        unsafe {
            LocalFree(self.0);
        }
    }
}
fn error(stage: &str) -> String {
    format!("{stage}: {}", std::io::Error::last_os_error())
}
fn sid_string(sid: PSID) -> Result<String, String> {
    // SAFETY: callers pass a SID from an OS-owned validated token/descriptor.
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err("native SID is invalid".into());
    }
    let mut raw = null_mut();
    // SAFETY: valid SID and pointer to a writable output allocation slot.
    if unsafe { ConvertSidToStringSidW(sid, &mut raw) } == 0 {
        return Err(error("convert-user-sid"));
    }
    let memory = LocalMemory(raw.cast());
    let mut len = 0;
    // SAFETY: a successful conversion returns a NUL-terminated SID string.
    while len < 192 && unsafe { *raw.add(len) } != 0 {
        len += 1;
    }
    if len == 192 {
        return Err("native SID string exceeds its bound".into());
    }
    // SAFETY: the preceding walk established the initialized string length.
    let value = String::from_utf16(unsafe { std::slice::from_raw_parts(raw, len) })
        .map_err(|_| "native SID string is invalid UTF16".to_owned())?;
    drop(memory);
    Ok(value)
}
fn current_sid() -> Result<String, String> {
    let mut raw = null_mut();
    // SAFETY: process pseudo-handle is valid; TOKEN_QUERY grants no write access.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw) } == 0 {
        return Err(error("open-current-token"));
    }
    let token = Handle(raw);
    let mut required = 0;
    // SAFETY: documented size-only query with a null buffer and zero capacity.
    unsafe {
        GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut required);
    }
    if required < std::mem::size_of::<TOKEN_USER>() as u32 || required > 64 * 1024 {
        return Err("native user token size is invalid".into());
    }
    let mut storage = vec![0usize; (required as usize).div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: aligned initialized storage is at least the requested byte count.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            storage.as_mut_ptr().cast(),
            required,
            &mut required,
        )
    } == 0
    {
        return Err(error("read-current-token"));
    }
    // SAFETY: successful TokenUser query produced a properly aligned TOKEN_USER.
    let user = unsafe { &*storage.as_ptr().cast::<TOKEN_USER>() };
    sid_string(user.User.Sid)
}
struct Directory {
    handle: Handle,
    original: String,
    identity: (u64, [u8; 16]),
    sid: String,
}
fn open_directory(path: &str) -> Result<Handle, String> {
    // Bind a live object before resolving aliases. A lexical absolute path
    // avoids canonicalize/open reauthorization and supports long DOS/UNC paths.
    let absolute = std::path::absolute(path).map_err(|e| format!("bind-directory: {e}"))?;
    let raw = absolute
        .to_str()
        .ok_or("directory path is not valid UTF16")?
        .replace('/', "\\");
    let extended = if raw.starts_with("\\\\?\\") {
        raw
    } else if let Some(unc) = raw.strip_prefix("\\\\") {
        format!("\\\\?\\UNC\\{unc}")
    } else {
        format!("\\\\?\\{raw}")
    };
    let wide: Vec<u16> = extended.encode_utf16().chain([0]).collect();
    // SAFETY: terminated path, fixed flags, no security/template pointer.
    let raw = unsafe {
        CreateFileW(
            wide.as_ptr(),
            READ_CONTROL | WRITE_DAC | FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            null_mut(),
        )
    };
    if raw == INVALID_HANDLE_VALUE {
        return Err(error("open-directory-acl"));
    }
    let handle = Handle(raw);
    let mut basic = FILE_BASIC_INFO::default();
    // SAFETY: live directory handle and exactly sized initialized result structure.
    if unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileBasicInfo,
            (&mut basic as *mut FILE_BASIC_INFO).cast(),
            std::mem::size_of::<FILE_BASIC_INFO>() as u32,
        )
    } == 0
    {
        return Err(error("read-directory-kind"));
    }
    if basic.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err("private worker directory is a reparse point".into());
    }
    if basic.FileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0 {
        return Err("private worker target is not a directory".into());
    }
    Ok(handle)
}
fn file_id(handle: &Handle) -> Result<(u64, [u8; 16]), String> {
    let mut info = FILE_ID_INFO::default();
    // SAFETY: live handle and exactly sized initialized FileIdInfo result.
    if unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileIdInfo,
            (&mut info as *mut FILE_ID_INFO).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(error("read-directory-identity"));
    }
    Ok((info.VolumeSerialNumber, info.FileId.Identifier))
}
impl Security for Directory {
    fn read(&self) -> Result<Snapshot, String> {
        let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
        let mut acl: *mut ACL = null_mut();
        // SAFETY: live file handle; OS allocates descriptor and ACL points into it.
        let status = unsafe {
            GetSecurityInfo(
                self.handle.0,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut acl,
                null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 {
            return Err(format!(
                "read-directory-acl: {}",
                std::io::Error::from_raw_os_error(status as i32)
            ));
        }
        let _memory = LocalMemory(descriptor);
        if descriptor.is_null() {
            return Err("native security descriptor is missing".into());
        }
        // SAFETY: GetSecurityInfo returned a valid security descriptor.
        let bytes = unsafe { GetSecurityDescriptorLength(descriptor) };
        if bytes == 0 || bytes > 1024 * 1024 {
            return Err("native security descriptor exceeds its bound".into());
        }
        let mut control = 0;
        let mut revision = 0;
        // SAFETY: valid descriptor and initialized control/revision outputs.
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0 {
            return Err(error("read-acl-protection"));
        }
        let mut rules = Vec::new();
        if !acl.is_null() {
            // SAFETY: OS returned this descriptor-owned ACL.
            if unsafe { IsValidAcl(acl) } == 0 {
                return Err("native directory ACL is invalid".into());
            }
            // SAFETY: ACL validity establishes the header.
            let count = unsafe { (*acl).AceCount };
            for index in 0..u32::from(count) {
                let mut entry = null_mut();
                // SAFETY: index is strictly bounded by this valid ACL's AceCount.
                if unsafe { GetAce(acl, index, &mut entry) } == 0 {
                    return Err(error("read-acl-rule"));
                }
                // SAFETY: successful GetAce returns a descriptor-owned ACE header.
                let header = unsafe { &*entry.cast::<ACE_HEADER>() };
                let kind = u32::from(header.AceType);
                if kind != ACCESS_ALLOWED_ACE_TYPE && kind != ACCESS_DENIED_ACE_TYPE {
                    return Err(
                        "unsupported native ACL rule; preserving security descriptor".into(),
                    );
                }
                if usize::from(header.AceSize) < std::mem::size_of::<ACCESS_ALLOWED_ACE>() {
                    return Err("native ACL rule is truncated".into());
                }
                let ace = entry.cast::<ACCESS_ALLOWED_ACE>();
                // SAFETY: allow and deny basic ACEs share this validated layout.
                let rights = unsafe { (*ace).Mask };
                // SAFETY: SidStart begins the SID inside the OS-validated ACE.
                let sid = unsafe { addr_of!((*ace).SidStart) }.cast_mut().cast();
                rules.push(Rule {
                    sid: sid_string(sid)?,
                    allow: kind == ACCESS_ALLOWED_ACE_TYPE,
                    rights,
                });
            }
        }
        Ok(Snapshot {
            protected: control & SE_DACL_PROTECTED != 0,
            rules,
        })
    }
    fn apply(&self) -> Result<(), String> {
        let sddl = format!(
            "D:P(A;OICI;FA;;;{})(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)",
            self.sid
        );
        let wide: Vec<u16> = sddl.encode_utf16().chain([0]).collect();
        let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
        // SAFETY: trusted SID string, fixed SDDL, terminated input and writable output.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                null_mut(),
            )
        } == 0
        {
            return Err(error("build-private-acl"));
        }
        let _memory = LocalMemory(descriptor);
        let mut present = 0;
        let mut defaulted = 0;
        let mut acl = null_mut();
        // SAFETY: successful conversion returned a valid descriptor.
        if unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut acl, &mut defaulted) }
            == 0
        {
            return Err(error("read-new-private-acl"));
        }
        if present == 0 || acl.is_null() {
            return Err("refusing a missing or null private DACL".into());
        }
        // SAFETY: same live directory object; DACL-only write preserves owner/group.
        // Explicit READ_CONTROL|WRITE_DAC and shared access permit child inheritance.
        let status = unsafe {
            SetSecurityInfo(
                self.handle.0,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                acl,
                null(),
            )
        };
        if status != 0 {
            return Err(format!(
                "apply-private-acl: {}",
                std::io::Error::from_raw_os_error(status as i32)
            ));
        }
        Ok(())
    }
}
pub(super) fn protect(directory: &str) -> Result<(), String> {
    protect_bound(directory, |_| Ok(()))
}
fn protect_bound(
    directory: &str,
    after_canonicalize: impl FnOnce(&str) -> Result<(), String>,
) -> Result<(), String> {
    let requested = open_directory(directory)?;
    let expected = file_id(&requested)?;
    let canonical =
        std::fs::canonicalize(directory).map_err(|e| format!("resolve-directory: {e}"))?;
    let canonical = canonical
        .to_str()
        .ok_or("canonical directory is not valid UTF16")?;
    after_canonicalize(canonical)?;
    let handle = open_directory(canonical)?;
    if file_id(&handle)? != expected {
        return Err("worker directory identity changed before ACL protection".into());
    }
    let sid = current_sid()?;
    let mut allowed = vec![sid.clone(), "S-1-5-18".into(), "S-1-5-32-544".into()];
    allowed.sort();
    allowed.dedup();
    let security = Directory {
        handle,
        original: directory.into(),
        identity: expected,
        sid,
    };
    super::acl_policy::protect(&security, &allowed)?;
    // The original path must still select the held object after verification.
    let after = open_directory(&security.original)?;
    if file_id(&after)? != security.identity {
        return Err("worker directory identity changed during ACL protection".into());
    }
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    fn snapshot(path: &std::path::Path) -> Snapshot {
        let handle = open_directory(path.to_str().unwrap()).unwrap();
        let identity = file_id(&handle).unwrap();
        Directory {
            handle,
            identity,
            original: path.to_str().unwrap().into(),
            sid: current_sid().unwrap(),
        }
        .read()
        .unwrap()
    }
    #[test]
    fn replacement_between_canonicalize_and_open_is_rejected_before_acl_mutation() {
        let root = tempfile::tempdir().unwrap();
        let requested = root.path().join("requested");
        let retired = root.path().join("held-original");
        let foreign = root.path().join("foreign");
        std::fs::create_dir_all(&requested).unwrap();
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(requested.join("marker"), "owned-original").unwrap();
        std::fs::write(foreign.join("marker"), "foreign-preserved").unwrap();
        let before_owned = snapshot(&requested);
        let before_foreign = snapshot(&foreign);
        let error = protect_bound(requested.to_str().unwrap(), |_| {
            std::fs::rename(&requested, &retired).unwrap();
            std::fs::rename(&foreign, &requested).unwrap();
            Ok(())
        })
        .unwrap_err();
        assert!(error.contains("identity changed before"), "{error}");
        assert_eq!(snapshot(&requested), before_foreign);
        assert_eq!(snapshot(&retired), before_owned);
        assert_eq!(
            std::fs::read_to_string(requested.join("marker")).unwrap(),
            "foreign-preserved"
        );
        assert_eq!(
            std::fs::read_to_string(retired.join("marker")).unwrap(),
            "owned-original"
        );
    }
}
